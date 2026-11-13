import Foundation

/// A change to the database of record that a replication member applied.
public enum ReplicatedChange: Sendable {
  /// One log entry was applied: its writes are in the database.
  case entry(ReplicationEntry)
  /// A leader's snapshot replaced the database up to this index.
  case snapshot(index: Number)
}

/// A request a follower forwarded to this member (spec section 9.9, `forward`).
public struct ForwardedRequest: Sendable {
  /// The follower that forwarded it and expects a `forwarded` answer.
  public var sender: String
  /// The writes to append, empty when the notes carry a request to run.
  public var batch: WriteBatch
  /// The notes to append, or the request to run.
  public var notes: [Value]
}

/// The leader's answer to a forwarded request.
public enum ForwardedReply: Sendable {
  /// The entry committed at this index.
  case committed(index: Number)
  /// The request did not commit.
  case failed(ReplicationError)
}

/// What a member knows about its place in the group.
public struct ReplicationStatus: Sendable, Equatable {
  /// The member's current role.
  public var role: ReplicationRole
  /// The latest term the member has seen.
  public var term: Number
  /// The leader the member knows, if any.
  public var leader: String?
  /// The highest index known to be committed.
  public var commit: Number
  /// The highest index applied to the database of record.
  public var applied: Number
}

/// Runs one member's `ReplicationNode` for an application and a transport.
///
/// The node is a state machine that takes calls and returns messages. The
/// coordinator turns it into streams and awaitable operations:
///
/// - A transport feeds it `receive` and `tick`, and sends what `outgoing` yields.
/// - An application appends with `appendAsLeader`, or forwards with `forward`
///   when it is not the leader, and each returns the entry once this member has
///   applied it (spec section 9.6).
/// - `changes` yields every applied entry in index order, the same on every
///   member, so the application can refresh what depends on the database.
///
/// The coordinator serializes every call into the node: the node suspends
/// while it reads and writes its log, and two calls interleaving there would
/// corrupt it.
public actor ReplicationCoordinator {
  private static let retainedEntries = 256

  /// The id of this member.
  public nonisolated let me: String

  /// Messages the transport must send, each to one member. A message may be
  /// lost in transit; the protocol tolerates it.
  public nonisolated let outgoing: AsyncStream<PeerMessage>

  /// Every change applied to the database of record, in order. One consumer.
  public nonisolated let changes: AsyncStream<ReplicatedChange>

  /// Requests followers forwarded to this member, in arrival order. One
  /// consumer, which answers each with `respond`.
  public nonisolated let forwardRequests: AsyncStream<ForwardedRequest>

  /// The live members (spec section 9.5) each time the list changes.
  public nonisolated let liveMemberChanges: AsyncStream<[Member]>

  private let node: ReplicationNode
  private let timeout: Duration
  private let nodeGate = AsyncGate()
  private let forwardGate = AsyncGate()
  private let outgoingSink: AsyncStream<PeerMessage>.Continuation
  private let changeSink: AsyncStream<ReplicatedChange>.Continuation
  private let forwardSink: AsyncStream<ForwardedRequest>.Continuation
  private let liveSink: AsyncStream<[Member]>.Continuation
  private var live: [Member]
  private var recent: [Number: ReplicationEntry] = [:]
  private var recentOrder: [Number] = []
  private var entryWaiters: [UUID: (index: Number, sink: AsyncStream<ReplicationEntry>.Continuation)] = [:]
  private var pendingReply: AsyncStream<ForwardedReply>.Continuation?

  /// Opens the member's replication log and prepares its state machine.
  ///
  /// - Parameters:
  ///   - group: The group and which member this is.
  ///   - database: This member's database of record.
  ///   - replicationDirectory: The directory of the member's log database.
  ///   - timeout: How long an append or forward waits for its entry to commit
  ///     before it fails with `ReplicationError.timedOut`.
  public init(group: Group, database: Database, replicationDirectory: URL, timeout: Duration = .seconds(10)) throws {
    node = try ReplicationNode(group: group, database: database, replicationDirectory: replicationDirectory)
    me = group.me
    self.timeout = timeout
    live = group.members.filter { $0.id == group.me }
    (outgoing, outgoingSink) = AsyncStream.makeStream(of: PeerMessage.self, bufferingPolicy: .unbounded)
    (changes, changeSink) = AsyncStream.makeStream(of: ReplicatedChange.self, bufferingPolicy: .unbounded)
    (forwardRequests, forwardSink) = AsyncStream.makeStream(of: ForwardedRequest.self, bufferingPolicy: .unbounded)
    (liveMemberChanges, liveSink) = AsyncStream.makeStream(of: [Member].self, bufferingPolicy: .bufferingNewest(1))
  }

  /// Recovers the member's state. Call once before anything else.
  public func start() async throws {
    try await node.start()
    try await refreshLive()
  }

  /// Closes the replication log.
  public func close() async throws { try await node.close() }

  /// The member's role, term, leader and progress.
  public func status() async -> ReplicationStatus {
    let status = await node.status()
    return ReplicationStatus(role: status.role, term: status.term, leader: status.leader, commit: status.commit, applied: status.applied)
  }

  /// The group's current member list (spec section 9.8).
  public func members() async -> [Member] { await node.currentMembers() }

  /// The members the leader heard from lately, this member included, as of the
  /// last `tick`. `liveMemberChanges` yields it when it changes.
  public var liveMembers: [Member] { live }

  /// Advances the member's timers: heartbeats, elections. A transport calls it
  /// every few tens of milliseconds.
  public func tick() async throws {
    try await nodeGate.run { try await self.step { try await $0.tick() } }
    try await refreshLive()
  }

  /// Takes one message from another member.
  ///
  /// `forward` and `forwarded` are the coordinator's own: it queues a forward
  /// for `forwardRequests` and hands a `forwarded` answer to the waiting
  /// `forward` call. Every other message goes to the state machine.
  ///
  /// - Throws: When the sender is not a member, or the state machine fails.
  public func receive(from sender: String, message: ReplicationMessage) async throws {
    guard await node.currentMembers().contains(where: { $0.id == sender }) else {
      throw KwiftDBError.invalidEncoding("message sender is not a group member")
    }
    switch message {
    case .forward(let payload, let notes):
      forwardSink.yield(ForwardedRequest(sender: sender, batch: try WriteBatchCodec.decode(payload), notes: notes))
    case .forwarded(let index, let failure):
      let reply: ForwardedReply = index.map { .committed(index: $0) } ?? .failed(ReplicationError(forwardedFailure: failure ?? ""))
      pendingReply?.yield(reply)
    default:
      try await nodeGate.run { try await self.step { try await $0.receive(from: sender, message: message) } }
    }
  }

  /// Answers a forwarded request.
  public func respond(to request: ForwardedRequest, with reply: ForwardedReply) {
    let message: ReplicationMessage =
      switch reply {
      case .committed(let index): .forwarded(index: index, failure: nil)
      case .failed(let error): .forwarded(index: nil, failure: error.forwardedFailure)
      }
    outgoingSink.yield(PeerMessage(member: request.sender, message: message))
  }

  /// Appends an entry as the leader and returns it once this member has applied
  /// it, which is once a majority holds it (spec section 9.6).
  ///
  /// Waits first until the leader has applied every earlier entry, so that what
  /// the caller read to produce its writes is current.
  ///
  /// - Throws: `ReplicationError.unavailable` when this member is not the
  ///   leader, or stopped being it before the entry committed.
  public func appendAsLeader(_ batch: WriteBatch, notes: [Value]) async throws -> ReplicationEntry {
    try await awaitLeaderReady()
    let (index, term) = try await nodeGate.run { try await self.submit(batch, notes: notes) }
    let entry = try await entry(at: index)
    let current = await node.status()
    guard current.role == .leader, current.term == term, entry.term == term else { throw ReplicationError.unavailable }
    return entry
  }

  /// Forwards an entry to the leader and returns it once this member has
  /// applied it (spec section 9.6). One forward is in flight at a time.
  ///
  /// - Throws: `ReplicationError.unavailable` with no leader known, `timedOut`
  ///   when the leader does not answer, `refused` with the leader's reason.
  public func forward(_ batch: WriteBatch, notes: [Value]) async throws -> ReplicationEntry {
    let index = try await forwardGate.run { try await self.sendForward(batch, notes: notes) }
    return try await entry(at: index)
  }

  private func sendForward(_ batch: WriteBatch, notes: [Value]) async throws -> Number {
    let status = await node.status()
    guard let leader = status.leader, leader != me else { throw ReplicationError.unavailable }
    let (replies, sink) = AsyncStream.makeStream(of: ForwardedReply.self, bufferingPolicy: .bufferingNewest(1))
    pendingReply = sink
    defer { pendingReply = nil }
    outgoingSink.yield(PeerMessage(member: leader, message: .forward(batch: WriteBatchCodec.encode(batch), notes: notes)))
    switch await Self.first(of: replies, within: timeout) {
    case .committed(let index)?: return index
    case .failed(let error)?: throw error
    case nil: throw ReplicationError.timedOut
    }
  }

  private func submit(_ batch: WriteBatch, notes: [Value]) async throws -> (Number, Number) {
    let status = await node.status()
    guard status.role == .leader else { throw ReplicationError.unavailable }
    guard case .appended(let index) = try await node.submit(batch, notes: notes) else { throw ReplicationError.unavailable }
    try await flush()
    return (index, status.term)
  }

  /// Waits until this member is the leader and has applied every entry in its
  /// log, so a read it makes sees every committed write (spec section 9.4: the
  /// entry a new leader writes commits the earlier ones).
  ///
  /// - Throws: `ReplicationError.unavailable` when this member is not the leader.
  public func awaitLeaderReady() async throws {
    let status = await node.status()
    guard status.role == .leader else { throw ReplicationError.unavailable }
    if status.applied < status.last { _ = try await entry(at: status.last) }
    guard await node.status().role == .leader else { throw ReplicationError.unavailable }
  }

  /// Runs one state-machine call, then sends its messages and takes its events.
  private func step(_ call: (ReplicationNode) async throws -> [PeerMessage]) async throws {
    for message in try await call(node) { outgoingSink.yield(message) }
    try await flush()
  }

  private func flush() async throws {
    for message in await node.takeOutgoing() { outgoingSink.yield(message) }
    for event in await node.takeEvents() {
      switch event {
      case .applied(let entry): applied(entry)
      case .installed(let index): changeSink.yield(.snapshot(index: index))
      case .becameLeader, .followedLeader, .forwarded: break
      }
    }
  }

  private func applied(_ entry: ReplicationEntry) {
    recent[entry.index] = entry
    recentOrder.append(entry.index)
    if recentOrder.count > Self.retainedEntries { recent.removeValue(forKey: recentOrder.removeFirst()) }
    for (id, waiter) in entryWaiters where waiter.index == entry.index {
      waiter.sink.yield(entry)
      waiter.sink.finish()
      entryWaiters.removeValue(forKey: id)
    }
    changeSink.yield(.entry(entry))
  }

  /// The entry at `index` once this member has applied it.
  private func entry(at index: Number) async throws -> ReplicationEntry {
    if let entry = recent[index] { return entry }
    let (entries, sink) = AsyncStream.makeStream(of: ReplicationEntry.self, bufferingPolicy: .bufferingNewest(1))
    let id = UUID()
    entryWaiters[id] = (index, sink)
    defer { entryWaiters.removeValue(forKey: id) }
    guard let entry = await Self.first(of: entries, within: timeout) else { throw ReplicationError.timedOut }
    return entry
  }

  private func refreshLive() async throws {
    let now = await node.liveMembers()
    if now != live {
      live = now
      liveSink.yield(now)
    }
  }

  /// The first element of `stream`, or nil when `timeout` passes or the stream
  /// ends first.
  private static func first<T: Sendable>(of stream: AsyncStream<T>, within timeout: Duration) async -> T? {
    await withTaskGroup(of: T?.self) { group in
      group.addTask {
        for await element in stream { return element }
        return nil
      }
      group.addTask {
        try? await Task.sleep(for: timeout)
        return nil
      }
      let first = await group.next() ?? nil
      group.cancelAll()
      return first
    }
  }
}
