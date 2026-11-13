import Foundation

/// The current consensus role of one replication member.
public enum ReplicationRole: Sendable, Equatable {
  /// Follows the current leader or waits for an election timeout.
  case follower
  /// Requests votes for a new term.
  case candidate
  /// Appends entries and advances commit after a majority acknowledges them.
  case leader
}

/// One message addressed to a specific replication peer.
public struct PeerMessage: Sendable {
  /// Destination member id.
  public var member: String
  /// Protocol message to send over the application's transport.
  public var message: ReplicationMessage
}

/// A visible event after a replicated log entry is applied locally.
public enum ReplicationEvent: Sendable {
  /// An entry was applied to the database of record. Its writes are in the
  /// database; its notes are opaque values for the application.
  case applied(ReplicationEntry)
  /// A leader's snapshot replaced the database of record up to this index.
  /// No entries were applied one by one, so readers must refresh everything.
  case installed(index: Number)
  /// This member became the leader for the term.
  case becameLeader(term: Number)
  /// A newer or same-term leader took control from this member.
  case followedLeader(id: String, term: Number)
  /// A forwarded mutation completed or failed at the known leader.
  case forwarded(index: Number?, failure: String?)
}

/// The immediate disposition of an application batch submitted to a member.
public enum ReplicationSubmission: Sendable {
  /// The local leader appended the entry at this index.
  case appended(Number)
  /// The batch was forwarded to the known leader.
  case forwarding(to: String)
  /// No leader is currently known.
  case unavailable
}

/// Timings used by the transport-independent consensus state machine.
public enum ReplicationTuning {
  /// Minimum interval between leader heartbeats.
  public static let heartbeat: Duration = .milliseconds(250)
  /// Heartbeat interval expressed in seconds for Foundation date arithmetic.
  public static let heartbeatSeconds = 0.25
  /// Base election timeout; followers randomize between one and two times it.
  public static let election: Duration = .milliseconds(1_500)
  /// Base election timeout expressed in seconds for Foundation date arithmetic.
  public static let electionSeconds = 1.5
}

/// A single-member and Raft-style multi-member replication engine.
///
/// The engine owns the separate durable replication log but not a socket. The
/// application calls `tick` on its event loop, sends the returned peer messages,
/// and passes responses to `receive`. Durable term/vote state is synced before
/// votes are sent; entries are written before append acknowledgements; and the
/// main database applies each committed entry together with its applied index.
public actor ReplicationNode {
  private let group: Group
  private let database: Database
  private let log: ReplicationLog
  private var started = false
  private var role: ReplicationRole = .follower
  private var term = Number.zero
  private var vote: String?
  private var snapshot: ReplicationSnapshot?
  private var members: [Member]
  private var knownLeader: String?
  private var lastIndex = Number.zero
  private var commitIndex = Number.zero
  private var appliedIndex = Number.zero
  private var electionDeadline = Date.distantFuture
  private var lastHeartbeat = Date.distantPast
  private var votes: Set<String> = []
  private var nextIndex: [String: Number] = [:]
  private var matchIndex: [String: Number] = [:]
  private var lastHeard: [String: Date] = [:]
  private var leaderLive: [String] = []
  private var pendingForwarders: [Number: [String]] = [:]
  private var outgoing: [PeerMessage] = []
  private var events: [ReplicationEvent] = []
  private var installBuffer: SnapshotBuffer?
  private var majority: Int { members.count / 2 + 1 }

  /// Opens the primary database and this member's dedicated replication directory.
  public init(group: Group, database: Database, replicationDirectory: URL) throws {
    self.group = group
    self.database = database
    log = try ReplicationLog(directory: replicationDirectory)
    members = group.members
  }

  /// Recovers local state and establishes the initial follower or single-member leader.
  public func start(at date: Date = Date()) async throws {
    guard !started else { return }
    let state = try await log.state()
    term = state.term
    vote = state.vote
    snapshot = state.snapshot
    if let snapshot { members = snapshot.members }
    let entries = try await log.entries(after: snapshot?.index ?? .zero)
    lastIndex = entries.last?.index ?? snapshot?.index ?? .zero
    commitIndex = try await readAppliedIndex()
    appliedIndex = commitIndex

    if snapshot == nil, group.members.first?.id == group.me {
      let initial = ReplicationSnapshot(index: Number(1), term: Number(1), members: group.members)
      snapshot = initial
      members = group.members
      term = Number(1)
      vote = nil
      lastIndex = Number(1)
      commitIndex = Number(1)
      appliedIndex = Number(1)
      try await log.store(snapshot: initial)
      try await log.store(term: term, vote: nil)
      try await storeAppliedIndex(appliedIndex)
    }
    if members.count == 1 && members.first?.id == group.me {
      role = .leader
      knownLeader = group.me
      nextIndex[group.me] = try Self.incremented(lastIndex)
      matchIndex[group.me] = lastIndex
    } else {
      role = .follower
      resetElectionDeadline(from: date)
    }
    started = true
  }

  /// Returns role, term, leader, commit and applied indexes for status displays.
  public func status() -> (role: ReplicationRole, term: Number, leader: String?, commit: Number, applied: Number, last: Number) {
    (role, term, knownLeader, commitIndex, appliedIndex, lastIndex)
  }

  /// The members of the group's current list, in list order (spec section 9.8).
  public func currentMembers() -> [Member] { members }

  /// The members the leader last heard from within the election timeout, this
  /// member included (spec section 9.5).
  ///
  /// The leader computes the list from its acknowledgements. A follower reports
  /// the list of the leader's latest `append` while it still hears the leader,
  /// and otherwise only itself.
  public func liveMembers(at date: Date = Date()) -> [Member] {
    let ids: Set<String>
    switch role {
    case .leader:
      ids = Set(members.filter { isLive($0.id, at: date) }.map(\.id))
    case .follower where date < electionDeadline:
      ids = Set(leaderLive).union([group.me])
    case .follower, .candidate:
      ids = [group.me]
    }
    return members.filter { ids.contains($0.id) }
  }

  /// Takes the messages queued by `submit` and `changeMembers`, which return
  /// only a disposition. `tick` and `receive` return their own.
  public func takeOutgoing() -> [PeerMessage] { takeMessages() }

  /// Advances timers and returns heartbeats or election requests that are due.
  public func tick(at date: Date = Date()) async throws -> [PeerMessage] {
    try ensureStarted()
    if role == .leader {
      // One decision for every member: `sendAppend` moves `lastHeartbeat`, so
      // checking it per member would starve all but the first.
      if date.timeIntervalSince(lastHeartbeat) >= ReplicationTuning.heartbeatSeconds {
        for member in members where member.id != group.me { try await sendAppend(to: member.id, at: date) }
        lastHeartbeat = date
      }
    } else if date >= electionDeadline && lastIndex > .zero && members.contains(where: { $0.id == group.me }) {
      try await beginElection(at: date)
    }
    return takeMessages()
  }

  /// Applies one consensus message and returns all messages this node must send.
  public func receive(from sender: String, message: ReplicationMessage, at date: Date = Date()) async throws -> [PeerMessage] {
    try ensureStarted()
    guard members.contains(where: { $0.id == sender }) else {
      throw KwiftDBError.invalidEncoding("message sender is not a group member")
    }
    switch message {
    case .vote(let incomingTerm, let candidate, let candidateIndex, let candidateLastTerm):
      try await handleVote(from: sender, term: incomingTerm, candidate: candidate, lastIndex: candidateIndex, lastTerm: candidateLastTerm, at: date)
    case .voted(let incomingTerm, let granted):
      try await handleVoted(from: sender, term: incomingTerm, granted: granted, at: date)
    case .append(let incomingTerm, let leader, let previousIndex, let previousTerm, let entries, let leaderCommit, let live):
      try await handleAppend(from: sender, term: incomingTerm, leader: leader, previousIndex: previousIndex, previousTerm: previousTerm, entries: entries, leaderCommit: leaderCommit, live: live, at: date)
    case .appended(let incomingTerm, let success, let last):
      try await handleAppended(from: sender, term: incomingTerm, success: success, last: last, at: date)
    case .install(let incomingTerm, let leader, let index, let indexTerm, let newMembers, let offset, let pairs, let done):
      try await handleInstall(from: sender, term: incomingTerm, leader: leader, index: index, indexTerm: indexTerm, members: newMembers, offset: offset, pairs: pairs, done: done, at: date)
    case .installed(let incomingTerm):
      try await handleInstalled(from: sender, term: incomingTerm, at: date)
    case .forward(let payload, let notes):
      try await handleForward(from: sender, payload: payload, notes: notes)
    case .forwarded(let index, let failure):
      events.append(.forwarded(index: index, failure: failure))
    }
    return takeMessages()
  }

  /// Appends a batch locally as leader or forwards it to the known leader.
  public func submit(_ batch: WriteBatch, notes: [Value] = []) async throws -> ReplicationSubmission {
    try ensureStarted()
    guard !batch.isEmpty || !notes.isEmpty else { return .unavailable }
    if role == .leader {
      let index = try await appendLocal(batch, notes: notes)
      if members.count == 1 {
        commitIndex = index
        try await applyCommittedEntries()
      } else {
        for member in members where member.id != group.me { try await sendAppend(to: member.id, at: Date()) }
      }
      return .appended(index)
    }
    guard let knownLeader, knownLeader != group.me else { return .unavailable }
    outgoing.append(PeerMessage(member: knownLeader, message: .forward(batch: WriteBatchCodec.encode(batch), notes: notes)))
    return .forwarding(to: knownLeader)
  }

  /// Appends one addition or removal to the group membership through the log.
  public func changeMembers(_ newMembers: [Member]) async throws -> Number {
    try ensureStarted()
    guard role == .leader else { throw KwiftDBError.table("only the replication leader can change members") }
    guard !newMembers.isEmpty, Set(newMembers.map(\.id)).count == newMembers.count,
          newMembers.allSatisfy({ !$0.id.isEmpty }) else {
      throw KwiftDBError.invalidSchema("replication member ids must be unique and non-empty")
    }
    let oldIDs = Set(members.map(\.id))
    let newIDs = Set(newMembers.map(\.id))
    guard oldIDs.subtracting(newIDs).count + newIDs.subtracting(oldIDs).count == 1 else {
      throw KwiftDBError.table("membership changes add or remove exactly one member")
    }
    guard try await log.entries(after: commitIndex).allSatisfy({ $0.members == nil }) else {
      throw KwiftDBError.table("a previous membership change is not committed")
    }
    let index = try await appendLocal(WriteBatch(), notes: [], members: newMembers)
    members = newMembers
    for member in newMembers where member.id != group.me && nextIndex[member.id] == nil {
      nextIndex[member.id] = Number(1)
      matchIndex[member.id] = .zero
    }
    if members.count == 1 {
      commitIndex = index
      try await applyCommittedEntries()
    } else {
      for member in members where member.id != group.me { try await sendAppend(to: member.id, at: Date()) }
    }
    return index
  }

  /// Takes application notes and leader-election events since the previous drain.
  public func takeEvents() -> [ReplicationEvent] {
    defer { events.removeAll(keepingCapacity: true) }
    return events
  }

  /// Closes the separate replication log database.
  public func close() async throws { try await log.close() }

  private func beginElection(at date: Date) async throws {
    term = try Self.incremented(term)
    vote = group.me
    role = .candidate
    knownLeader = nil
    votes = [group.me]
    try await log.store(term: term, vote: vote)
    resetElectionDeadline(from: date)
    let (index, lastTerm) = try await lastPosition()
    for member in members where member.id != group.me {
      outgoing.append(PeerMessage(member: member.id, message: .vote(term: term, candidate: group.me, lastIndex: index, lastTerm: lastTerm)))
    }
    if votes.count >= majority { try await becomeLeader(at: date) }
  }

  private func handleVote(from sender: String, term incoming: Number, candidate: String, lastIndex candidateIndex: Number, lastTerm candidateTerm: Number, at date: Date) async throws {
    guard sender == candidate else { return }
    if incoming > term {
      term = incoming
      vote = nil
      role = .follower
      knownLeader = nil
      try await log.store(term: term, vote: nil)
    }
    var granted = false
    if incoming >= term, members.contains(where: { $0.id == candidate }),
       vote == nil || vote == candidate {
      let (localIndex, localTerm) = try await lastPosition()
      if candidateTerm > localTerm || (candidateTerm == localTerm && candidateIndex >= localIndex) {
        vote = candidate
        try await log.store(term: term, vote: candidate)
        resetElectionDeadline(from: date)
        granted = true
      }
    }
    outgoing.append(PeerMessage(member: sender, message: .voted(term: term, granted: granted)))
  }

  private func handleVoted(from sender: String, term incoming: Number, granted: Bool, at date: Date) async throws {
    if incoming > term {
      term = incoming
      vote = nil
      role = .follower
      knownLeader = nil
      try await log.store(term: term, vote: nil)
      resetElectionDeadline(from: date)
      return
    }
    guard role == .candidate, incoming == term, granted, members.contains(where: { $0.id == sender }) else { return }
    votes.insert(sender)
    if votes.count >= majority { try await becomeLeader(at: date) }
  }

  private func becomeLeader(at date: Date) async throws {
    role = .leader
    knownLeader = group.me
    lastHeard = [:]
    events.append(.becameLeader(term: term))
    let next = try Self.incremented(lastIndex)
    for member in members where member.id != group.me {
      nextIndex[member.id] = next
      matchIndex[member.id] = .zero
    }
    matchIndex[group.me] = lastIndex
    _ = try await appendLocal(WriteBatch(), notes: [])
    if members.count == 1 {
      commitIndex = lastIndex
      try await applyCommittedEntries()
    } else {
      for member in members where member.id != group.me { try await sendAppend(to: member.id, at: date) }
    }
  }

  private func handleAppend(from sender: String, term incoming: Number, leader: String, previousIndex: Number, previousTerm: Number, entries: [ReplicationEntry], leaderCommit: Number, live: [String], at date: Date) async throws {
    if incoming < term {
      outgoing.append(PeerMessage(member: sender, message: .appended(term: term, success: false, last: lastIndex)))
      return
    }
    if incoming > term {
      term = incoming
      vote = nil
      try await log.store(term: term, vote: nil)
    }
    role = .follower
    knownLeader = leader
    resetElectionDeadline(from: date)
    if leader != sender { throw KwiftDBError.invalidEncoding("append sender does not match its leader id") }
    events.append(.followedLeader(id: leader, term: term))
    let localPreviousTerm = try await term(at: previousIndex)
    guard localPreviousTerm == previousTerm else {
      outgoing.append(PeerMessage(member: sender, message: .appended(term: term, success: false, last: lastIndex)))
      return
    }

    var expectedIndex = try Self.incremented(previousIndex)
    for incomingEntry in entries {
      guard incomingEntry.index == expectedIndex else {
        throw KwiftDBError.invalidEncoding("append entries are not contiguous")
      }
      expectedIndex = try Self.incremented(expectedIndex)
      if let local = try await log.entry(at: incomingEntry.index) {
        if local.term != incomingEntry.term {
          try await log.truncate(from: incomingEntry.index)
          try await log.append(incomingEntry)
        }
      } else if incomingEntry.index > (snapshot?.index ?? .zero) {
        try await log.append(incomingEntry)
      }
    }
    let retained = try await log.entries(after: snapshot?.index ?? .zero)
    lastIndex = retained.last?.index ?? snapshot?.index ?? .zero
    members = retained.last(where: { $0.members != nil })?.members ?? snapshot?.members ?? group.members
    let acceptedIndex = entries.last?.index ?? previousIndex
    let upperCommit = leaderCommit < acceptedIndex ? leaderCommit : acceptedIndex
    if upperCommit > commitIndex { commitIndex = upperCommit; try await applyCommittedEntries() }
    leaderLive = live
    outgoing.append(PeerMessage(member: sender, message: .appended(term: term, success: true, last: acceptedIndex)))
  }

  private func handleAppended(from sender: String, term incoming: Number, success: Bool, last: Number, at date: Date) async throws {
    if incoming > term {
      term = incoming
      vote = nil
      role = .follower
      knownLeader = nil
      try await log.store(term: term, vote: nil)
      resetElectionDeadline(from: date)
      return
    }
    guard role == .leader, incoming == term else { return }
    lastHeard[sender] = date
    if success {
      if last > (matchIndex[sender] ?? .zero) { matchIndex[sender] = last }
      nextIndex[sender] = try Self.incremented(last)
      try await advanceCommitIndex()
    } else {
      let backedUp = last < lastIndex ? last : lastIndex
      nextIndex[sender] = try Self.incremented(backedUp)
      try await sendAppend(to: sender, at: date)
    }
  }

  private func handleInstall(from sender: String, term incoming: Number, leader: String, index: Number, indexTerm: Number, members newMembers: [Member], offset: Number, pairs: [KeyValue], done: Bool, at date: Date) async throws {
    guard incoming >= term else { return }
    if incoming > term { term = incoming; vote = nil; try await log.store(term: term, vote: nil) }
    role = .follower
    knownLeader = leader
    resetElectionDeadline(from: date)
    guard leader == sender else { throw KwiftDBError.invalidEncoding("snapshot sender does not match its leader id") }
    let offsetValue = try Self.unsigned(offset)
    if offsetValue.isZero { installBuffer = SnapshotBuffer(index: index, term: indexTerm, members: newMembers, pairs: [], nextOffset: .zero) }
    guard var buffer = installBuffer, buffer.index == index, buffer.term == indexTerm,
          buffer.nextOffset == offsetValue else {
      throw KwiftDBError.invalidEncoding("snapshot pieces are missing or out of order")
    }
    buffer.pairs.append(contentsOf: pairs)
    buffer.nextOffset = buffer.nextOffset.adding(UInt64(pairs.count))
    installBuffer = buffer
    guard done else { return }
    var snapshotPairs = buffer.pairs
    let appliedKey = Self.appliedKey()
    let appliedValue = Array(try ValueCodec.encode(.number(index)))
    if let existing = snapshotPairs.firstIndex(where: { $0.key == appliedKey }) {
      snapshotPairs[existing].value = appliedValue
    } else {
      snapshotPairs.append(KeyValue(key: appliedKey, value: appliedValue))
    }
    snapshotPairs.sort { $0.key.lexicographicallyPrecedes($1.key) }
    try await database.replaceAll(with: snapshotPairs)
    let replacement = ReplicationSnapshot(index: index, term: indexTerm, members: newMembers)
    try await log.store(snapshot: replacement)
    snapshot = replacement
    members = newMembers
    lastIndex = index
    commitIndex = index
    appliedIndex = index
    installBuffer = nil
    events.append(.installed(index: index))
    outgoing.append(PeerMessage(member: sender, message: .installed(term: term)))
  }

  private func handleInstalled(from sender: String, term incoming: Number, at date: Date) async throws {
    if incoming > term { term = incoming; vote = nil; role = .follower; try await log.store(term: term, vote: nil); resetElectionDeadline(from: date); return }
    guard role == .leader, incoming == term else { return }
    let index = appliedIndex
    matchIndex[sender] = index
    nextIndex[sender] = try Self.incremented(index)
    try await sendAppend(to: sender, at: date)
  }

  private func handleForward(from sender: String, payload: Data, notes: [Value]) async throws {
    guard role == .leader else {
      if let knownLeader, knownLeader != group.me {
        outgoing.append(PeerMessage(member: knownLeader, message: .forward(batch: payload, notes: notes)))
      } else {
        outgoing.append(PeerMessage(member: sender, message: .forwarded(index: nil, failure: "Unavailable")))
      }
      return
    }
    let batch = try WriteBatchCodec.decode(payload)
    let index = try await appendLocal(batch, notes: notes)
    pendingForwarders[index, default: []].append(sender)
    if members.count == 1 {
      commitIndex = index
      try await applyCommittedEntries()
    } else {
      for member in members where member.id != group.me { try await sendAppend(to: member.id, at: Date()) }
    }
  }

  private func appendLocal(_ batch: WriteBatch, notes: [Value], members: [Member]? = nil) async throws -> Number {
    let index = try Self.incremented(lastIndex)
    let entry = ReplicationEntry(index: index, term: term, batch: batch, notes: notes, members: members)
    try await log.append(entry)
    lastIndex = index
    matchIndex[group.me] = index
    return index
  }

  private func sendAppend(to member: String, at date: Date) async throws {
    guard let next = nextIndex[member] else { return }
    if let snapshot, next <= snapshot.index {
      let freshSnapshot = ReplicationSnapshot(index: appliedIndex, term: try await term(at: appliedIndex), members: members)
      let pairs = try await database.allPairs()
      try await log.store(snapshot: freshSnapshot)
      self.snapshot = freshSnapshot
      outgoing.append(PeerMessage(member: member, message: .install(
        term: term, leader: group.me, index: freshSnapshot.index, indexTerm: freshSnapshot.term,
        members: freshSnapshot.members, offset: .zero, pairs: pairs, done: true
      )))
      return
    }
    let previousIndex = try Self.decremented(next)
    let previousTerm = try await term(at: previousIndex)
    let entries = try await log.entries(after: previousIndex)
    let live = members.filter { isLive($0.id, at: date) }.map(\.id)
    outgoing.append(PeerMessage(member: member, message: .append(
      term: term, leader: group.me, previousIndex: previousIndex, previousTerm: previousTerm,
      entries: entries, commit: commitIndex, live: live
    )))
    lastHeartbeat = date
  }

  private func isLive(_ id: String, at date: Date) -> Bool {
    id == group.me || date.timeIntervalSince(lastHeard[id] ?? .distantPast) <= ReplicationTuning.electionSeconds
  }

  private func advanceCommitIndex() async throws {
    let start = try Self.incremented(commitIndex)
    if start > lastIndex { return }
    var candidate = commitIndex
    for value in try await log.entries(after: commitIndex) {
      let acknowledgements = members.reduce(into: 0) { count, member in
        let index = member.id == group.me ? lastIndex : (matchIndex[member.id] ?? .zero)
        if index >= value.index { count += 1 }
      }
      if acknowledgements >= majority, value.term == term { candidate = value.index }
    }
    if candidate > commitIndex {
      commitIndex = candidate
      try await applyCommittedEntries()
      let completedForwarding = pendingForwarders.keys.filter { $0 <= commitIndex }
      for index in completedForwarding {
        for member in pendingForwarders[index] ?? [] {
          outgoing.append(PeerMessage(member: member, message: .forwarded(index: index, failure: nil)))
        }
        pendingForwarders.removeValue(forKey: index)
      }
      for member in members where member.id != group.me { try await sendAppend(to: member.id, at: Date()) }
    }
  }

  private func applyCommittedEntries() async throws {
    while appliedIndex < commitIndex {
      let next = try Self.incremented(appliedIndex)
      guard let entry = try await log.entry(at: next) else {
        if let snapshot, next <= snapshot.index { appliedIndex = next; continue }
        throw KwiftDBError.corruptStorage("committed replication entry is missing")
      }
      var batch = try entry.writeBatch()
      batch.put(Array(try ValueCodec.encode(.number(next))), for: Self.appliedKey())
      try await database.writeBatch(batch)
      appliedIndex = next
      events.append(.applied(entry))
      if let newMembers = entry.members { members = newMembers }
    }
    if !members.contains(where: { $0.id == group.me }) {
      role = .follower
      knownLeader = nil
    }
  }

  private func lastPosition() async throws -> (index: Number, term: Number) {
    if let last = try await log.entries(after: snapshot?.index ?? .zero).last { return (last.index, last.term) }
    if let snapshot { return (snapshot.index, snapshot.term) }
    return (.zero, .zero)
  }

  private func term(at index: Number) async throws -> Number {
    if index.isZero { return .zero }
    if let snapshot, snapshot.index == index { return snapshot.term }
    return try await log.entry(at: index)?.term ?? .zero
  }

  private func readAppliedIndex() async throws -> Number {
    guard let bytes = try await database.readKey(Self.appliedKey()) else { return snapshot?.index ?? .zero }
    guard case .number(let number) = try ValueCodec.decode(Data(bytes)) else {
      throw KwiftDBError.corruptStorage("replication applied index is not numeric")
    }
    return number
  }

  private func storeAppliedIndex(_ index: Number) async throws {
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.number(index))), for: Self.appliedKey())
    try await database.writeBatch(batch)
  }

  private static func appliedKey() -> [UInt8] {
    (try? KeyEncoder.encodeTuple([.text("r"), .text("applied")])) ?? []
  }

  private static func incremented(_ number: Number) throws -> Number {
    guard let text = number.unsignedIntegerText else { throw KwiftDBError.corruptStorage("replication counter is not unsigned") }
    return try Number(DatabaseUInt(text).incremented().description)
  }

  private static func decremented(_ number: Number) throws -> Number {
    guard let text = number.unsignedIntegerText, let value = try? DatabaseUInt(text), !value.isZero else {
      throw KwiftDBError.corruptStorage("replication index cannot be decremented")
    }
    return try Number(value.decremented().description)
  }

  private static func unsigned(_ number: Number) throws -> DatabaseUInt {
    guard let text = number.unsignedIntegerText else { throw KwiftDBError.invalidEncoding("expected unsigned replication index") }
    return try DatabaseUInt(text)
  }

  private func resetElectionDeadline(from date: Date) {
    let timeout = Double.random(in: ReplicationTuning.electionSeconds...(ReplicationTuning.electionSeconds * 2))
    electionDeadline = date.addingTimeInterval(timeout)
  }

  private func takeMessages() -> [PeerMessage] {
    defer { outgoing.removeAll(keepingCapacity: true) }
    return outgoing
  }

  private func ensureStarted() throws {
    if !started { throw KwiftDBError.io("replication node has not been started") }
  }
}

private struct SnapshotBuffer {
  let index: Number
  let term: Number
  let members: [Member]
  var pairs: [KeyValue]
  var nextOffset: DatabaseUInt
}
