import Foundation

/// One durable member of a replication group.
public struct Member: Sendable, Equatable {
  /// A stable, never-reused member id.
  public var id: String
  /// The address used by the application's transport binding.
  public var address: String

  /// Creates a member descriptor.
  public init(id: String, address: String) {
    self.id = id
    self.address = address
  }
}

/// The configured membership and local identity of one replication process.
public struct Group: Sendable, Equatable {
  /// Members in their current declaration order.
  public var members: [Member]
  /// Id of the local process.
  public var me: String

  /// Validates unique ids and requires `me` to name one configured member.
  public init(members: [Member], me: String) throws {
    guard !members.isEmpty, Set(members.map(\.id)).count == members.count,
          members.allSatisfy({ !$0.id.isEmpty }), members.contains(where: { $0.id == me }) else {
      throw KwiftDBError.invalidSchema("replication group needs unique member ids and a local member")
    }
    self.members = members
    self.me = me
  }

  /// The number of acknowledgements required to form a majority.
  public var majority: Int { members.count / 2 + 1 }
}

/// A persisted replication log entry.
public struct ReplicationEntry: Sendable, Equatable {
  /// Its one-based index in the group log.
  public var index: Number
  /// The term in which its leader appended it.
  public var term: Number
  /// A `WriteBatchCodec` payload with sequence zero.
  public var batch: Data
  /// Application-owned notes carried with the log entry.
  public var notes: [Value]
  /// A membership change, absent for ordinary entries.
  public var members: [Member]?

  /// Creates an entry, encoding a typed write batch into the replication format.
  public init(index: Number, term: Number, batch: WriteBatch, notes: [Value] = [], members: [Member]? = nil) {
    self.index = index
    self.term = term
    self.batch = WriteBatchCodec.encode(batch)
    self.notes = notes
    self.members = members
  }

  /// Creates an entry from an already encoded batch payload.
  public init(index: Number, term: Number, batchPayload: Data, notes: [Value] = [], members: [Member]? = nil) throws {
    _ = try WriteBatchCodec.decode(batchPayload)
    self.index = index
    self.term = term
    self.batch = batchPayload
    self.notes = notes
    self.members = members
  }

  /// Decodes the entry's ordered database operations.
  public func writeBatch() throws -> WriteBatch { try WriteBatchCodec.decode(batch) }
}

/// The last replicated entry compacted out of a member's log.
public struct ReplicationSnapshot: Sendable, Equatable {
  /// The last included log index.
  public var index: Number
  /// The term of the last included entry.
  public var term: Number
  /// Membership in effect at the snapshot.
  public var members: [Member]

  /// Creates a snapshot marker.
  public init(index: Number, term: Number, members: [Member]) {
    self.index = index
    self.term = term
    self.members = members
  }
}

/// The persistent term, vote, and compacted prefix for a replication log.
public struct ReplicationPersistentState: Sendable, Equatable {
  /// Highest term this member has seen.
  public var term: Number
  /// The member voted for in `term`, or `nil`.
  public var vote: String?
  /// The compacted log prefix, or `nil` before its first snapshot.
  public var snapshot: ReplicationSnapshot?
}

/// A peer-to-peer consensus or forwarding message.
public enum ReplicationMessage: Sendable {
  /// Requests a vote for a candidate's term and log position.
  case vote(term: Number, candidate: String, lastIndex: Number, lastTerm: Number)
  /// Answers a vote request.
  case voted(term: Number, granted: Bool)
  /// Sends a leader's contiguous log suffix and commit index.
  case append(term: Number, leader: String, previousIndex: Number, previousTerm: Number, entries: [ReplicationEntry], commit: Number, live: [String])
  /// Answers an append request with the last accepted index.
  case appended(term: Number, success: Bool, last: Number)
  /// Installs a piece of a consistent database snapshot.
  case install(term: Number, leader: String, index: Number, indexTerm: Number, members: [Member], offset: Number, pairs: [KeyValue], done: Bool)
  /// Acknowledges an installed snapshot.
  case installed(term: Number)
  /// Forwards an application mutation to the known leader.
  case forward(batch: Data, notes: [Value])
  /// Answers a forwarded mutation after application or refusal.
  case forwarded(index: Number?, failure: String?)

  /// Encodes the message as the ordered document format used by replication.
  public func encodedValue() throws -> Data {
    let fields: [DocumentField]
    switch self {
    case .vote(let term, let candidate, let lastIndex, let lastTerm):
      fields = [DocumentField("type", .text("vote")), DocumentField("term", .number(term)), DocumentField("candidate", .text(candidate)), DocumentField("lastIndex", .number(lastIndex)), DocumentField("lastTerm", .number(lastTerm))]
    case .voted(let term, let granted):
      fields = [DocumentField("type", .text("voted")), DocumentField("term", .number(term)), DocumentField("granted", .bool(granted))]
    case .append(let term, let leader, let previousIndex, let previousTerm, let entries, let commit, let live):
      fields = [
        DocumentField("type", .text("append")), DocumentField("term", .number(term)), DocumentField("leader", .text(leader)),
        DocumentField("previousIndex", .number(previousIndex)), DocumentField("previousTerm", .number(previousTerm)),
        DocumentField("entries", .list(try entries.map { .document(try Self.entryDocument($0)) })),
        DocumentField("commit", .number(commit)), DocumentField("live", .list(live.map(Value.text))),
      ]
    case .appended(let term, let success, let last):
      fields = [DocumentField("type", .text("appended")), DocumentField("term", .number(term)), DocumentField("success", .bool(success)), DocumentField("last", .number(last))]
    case .install(let term, let leader, let index, let indexTerm, let members, let offset, let pairs, let done):
      let pairValues = try pairs.map { try Document([DocumentField("key", .bytes($0.key)), DocumentField("value", .bytes($0.value))]) }
      fields = [
        DocumentField("type", .text("install")), DocumentField("term", .number(term)), DocumentField("leader", .text(leader)),
        DocumentField("index", .number(index)), DocumentField("indexTerm", .number(indexTerm)),
        DocumentField("members", .list(try Self.memberValues(members))), DocumentField("offset", .number(offset)),
        DocumentField("pairs", .list(pairValues.map(Value.document))), DocumentField("done", .bool(done)),
      ]
    case .installed(let term):
      fields = [DocumentField("type", .text("installed")), DocumentField("term", .number(term))]
    case .forward(let batch, let notes):
      fields = [DocumentField("type", .text("forward")), DocumentField("batch", .bytes(Array(batch))), DocumentField("notes", .list(notes))]
    case .forwarded(let index, let failure):
      if let index { fields = [DocumentField("type", .text("forwarded")), DocumentField("index", .number(index))] }
      else if let failure { fields = [DocumentField("type", .text("forwarded")), DocumentField("failure", .text(failure))] }
      else { throw KwiftDBError.invalidEncoding("forwarded reply needs an index or failure") }
    }
    return try ValueCodec.encode(.document(Document(fields: fields)))
  }

  /// Decodes a complete replication message and validates its field order.
  public static func decodeValue(_ data: Data) throws -> ReplicationMessage {
    guard case .document(let document) = try ValueCodec.decode(data), case .text(let type)? = document["type"] else {
      throw KwiftDBError.invalidEncoding("replication message is not a typed document")
    }
    switch type {
    case "vote":
      try fields(document, ["type", "term", "candidate", "lastIndex", "lastTerm"])
      return .vote(term: try number("term", document), candidate: try text("candidate", document), lastIndex: try number("lastIndex", document), lastTerm: try number("lastTerm", document))
    case "voted":
      try fields(document, ["type", "term", "granted"])
      return .voted(term: try number("term", document), granted: try bool("granted", document))
    case "append":
      try fields(document, ["type", "term", "leader", "previousIndex", "previousTerm", "entries", "commit", "live"])
      guard case .list(let entryValues)? = document["entries"], case .list(let liveValues)? = document["live"] else {
        throw KwiftDBError.invalidEncoding("append has malformed lists")
      }
      let entries = try entryValues.map { value -> ReplicationEntry in
        guard case .document(let record) = value else { throw KwiftDBError.invalidEncoding("malformed append entry") }
        return try entry(record)
      }
      let live = try liveValues.map { value -> String in
        guard case .text(let id) = value else { throw KwiftDBError.invalidEncoding("malformed live member") }
        return id
      }
      return .append(term: try number("term", document), leader: try text("leader", document), previousIndex: try number("previousIndex", document), previousTerm: try number("previousTerm", document), entries: entries, commit: try number("commit", document), live: live)
    case "appended":
      try fields(document, ["type", "term", "success", "last"])
      return .appended(term: try number("term", document), success: try bool("success", document), last: try number("last", document))
    case "install":
      try fields(document, ["type", "term", "leader", "index", "indexTerm", "members", "offset", "pairs", "done"])
      guard case .list(let memberValues)? = document["members"], case .list(let pairValues)? = document["pairs"] else {
        throw KwiftDBError.invalidEncoding("install has malformed lists")
      }
      let members = try memberValues.map { try member($0) }
      let pairs = try pairValues.map { value -> KeyValue in
        guard case .document(let pair) = value, case .bytes(let key)? = pair["key"], case .bytes(let bytes)? = pair["value"] else {
          throw KwiftDBError.invalidEncoding("malformed snapshot pair")
        }
        return KeyValue(key: key, value: bytes)
      }
      return .install(term: try number("term", document), leader: try text("leader", document), index: try number("index", document), indexTerm: try number("indexTerm", document), members: members, offset: try number("offset", document), pairs: pairs, done: try bool("done", document))
    case "installed":
      try fields(document, ["type", "term"])
      return .installed(term: try number("term", document))
    case "forward":
      try fields(document, ["type", "batch", "notes"])
      guard case .bytes(let batch)? = document["batch"], case .list(let notes)? = document["notes"] else {
        throw KwiftDBError.invalidEncoding("forward has malformed fields")
      }
      _ = try WriteBatchCodec.decode(Data(batch))
      return .forward(batch: Data(batch), notes: notes)
    case "forwarded":
      guard document.fields.first?.name == "type", document.fields.count == 2 else { throw KwiftDBError.invalidEncoding("malformed forwarded reply") }
      if case .number(let index)? = document["index"], document.fields[1].name == "index" { return .forwarded(index: index, failure: nil) }
      if case .text(let reason)? = document["failure"], document.fields[1].name == "failure" { return .forwarded(index: nil, failure: reason) }
      throw KwiftDBError.invalidEncoding("forwarded reply needs an index or failure")
    default: throw KwiftDBError.invalidEncoding("unknown replication message type \(type)")
    }
  }

  private static func entryDocument(_ entry: ReplicationEntry) throws -> Document {
    var fields = [
      DocumentField("index", .number(entry.index)), DocumentField("term", .number(entry.term)),
      DocumentField("batch", .bytes(Array(entry.batch))), DocumentField("notes", .list(entry.notes)),
    ]
    if let members = entry.members { fields.append(DocumentField("members", .list(try memberValues(members)))) }
    return try Document(fields)
  }

  private static func entry(_ document: Document) throws -> ReplicationEntry {
    guard case .number(let index)? = document["index"], case .number(let term)? = document["term"],
          case .bytes(let batch)? = document["batch"], case .list(let notes)? = document["notes"] else {
      throw KwiftDBError.invalidEncoding("malformed replication entry")
    }
    let members: [Member]?
    if case .list(let values)? = document["members"] { members = try values.map(member) }
    else { members = nil }
    return try ReplicationEntry(index: index, term: term, batchPayload: Data(batch), notes: notes, members: members)
  }

  fileprivate static func memberValues(_ members: [Member]) throws -> [Value] {
    try members.map { .document(try Document([DocumentField("id", .text($0.id)), DocumentField("address", .text($0.address))])) }
  }

  fileprivate static func member(_ value: Value) throws -> Member {
    guard case .document(let document) = value else { throw KwiftDBError.invalidEncoding("member is not a document") }
    return Member(id: try text("id", document), address: try text("address", document))
  }

  private static func fields(_ document: Document, _ names: [String]) throws {
    guard document.fields.map(\.name) == names else { throw KwiftDBError.invalidEncoding("replication fields are missing, extra, or out of order") }
  }

  private static func number(_ name: String, _ document: Document) throws -> Number {
    guard case .number(let value)? = document[name] else { throw KwiftDBError.invalidEncoding("replication field \(name) is not numeric") }
    return value
  }

  private static func text(_ name: String, _ document: Document) throws -> String {
    guard case .text(let value)? = document[name] else { throw KwiftDBError.invalidEncoding("replication field \(name) is not text") }
    return value
  }

  private static func bool(_ name: String, _ document: Document) throws -> Bool {
    guard case .bool(let value)? = document[name] else { throw KwiftDBError.invalidEncoding("replication field \(name) is not Boolean") }
    return value
  }
}

/// Encodes replication messages as length-prefixed database values.
public enum ReplicationFrameCodec {
  /// Creates one frame with a base-128 variable-length byte count.
  public static func encode(_ message: ReplicationMessage) throws -> Data {
    let payload = try message.encodedValue()
    var frame = Data(DatabaseUInt(UInt64(payload.count)).varintBytes())
    frame.append(payload)
    return frame
  }
}

/// Persisted state and log access for one member's `replication` directory.
public actor ReplicationLog {
  private let database: Database

  /// Opens the separate durable database used for term/vote and log records.
  public init(directory: URL) throws { database = try Database(directory: directory) }

  /// Reads term, vote, and snapshot metadata, using the initial empty values when absent.
  public func state() async throws -> ReplicationPersistentState {
    let term = try await readNumber(Self.stateKey("term")) ?? .zero
    let voteValue = try await readValue(Self.stateKey("vote"))
    let vote: String?
    if case .text(let id)? = voteValue { vote = id } else { vote = nil }
    let snapshotValue = try await readValue(Self.stateKey("snapshot"))
    let snapshot: ReplicationSnapshot?
    if let snapshotValue { snapshot = try Self.decodeSnapshot(snapshotValue) } else { snapshot = nil }
    return ReplicationPersistentState(term: term, vote: vote, snapshot: snapshot)
  }

  /// Persists a new term and vote in one synced batch before messages are sent.
  public func store(term: Number, vote: String?) async throws {
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.number(term))), for: Self.stateKey("term"))
    batch.put(Array(try ValueCodec.encode(vote.map(Value.text) ?? .null)), for: Self.stateKey("vote"))
    try await database.writeBatch(batch)
  }

  /// Appends a log entry under its one-based index.
  public func append(_ entry: ReplicationEntry) async throws {
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.document(Self.entryDocument(entry)))), for: try Self.entryKey(entry.index))
    try await database.writeBatch(batch)
  }

  /// Reads one retained entry, or `nil` when it is outside the current log.
  public func entry(at index: Number) async throws -> ReplicationEntry? {
    guard let value = try await readValue(Self.entryKey(index)) else { return nil }
    return try Self.decodeEntry(index: index, value: value)
  }

  /// Reads the retained suffix strictly after `index`, in numeric order.
  public func entries(after index: Number = .zero) async throws -> [ReplicationEntry] {
    let prefix = try KeyEncoder.encodeTuple([.text("r"), .text("log")])
    let rows = try await database.scanKeys(from: prefix, to: DocumentCollection.upperBound(after: prefix))
    return try rows.compactMap { row in
      let tuple = try KeyDecoder.decodeTuple(row.key)
      guard tuple.count == 3, case .number(let number) = tuple[2], number > index else { return nil }
      guard case .document(let value) = try ValueCodec.decode(Data(row.value)) else {
        throw KwiftDBError.corruptStorage("replication entry value is not a document")
      }
      return try Self.decodeEntry(index: number, value: .document(value))
    }.sorted { $0.index < $1.index }
  }

  /// Removes the entry at `index` and every later entry after a conflict.
  public func truncate(from index: Number) async throws {
    let prefix = try KeyEncoder.encodeTuple([.text("r"), .text("log")])
    var batch = WriteBatch()
    for row in try await database.scanKeys(from: prefix, to: DocumentCollection.upperBound(after: prefix)) {
      let tuple = try KeyDecoder.decodeTuple(row.key)
      if tuple.count == 3, case .number(let number) = tuple[2], number >= index { batch.delete(row.key) }
    }
    try await database.writeBatch(batch)
  }

  /// Replaces the snapshot marker and drops entries included by that snapshot.
  public func store(snapshot: ReplicationSnapshot) async throws {
    let value = try Self.snapshotValue(snapshot)
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(value)), for: Self.stateKey("snapshot"))
    let prefix = try KeyEncoder.encodeTuple([.text("r"), .text("log")])
    for row in try await database.scanKeys(from: prefix, to: DocumentCollection.upperBound(after: prefix)) {
      let tuple = try KeyDecoder.decodeTuple(row.key)
      if tuple.count == 3, case .number(let number) = tuple[2], number <= snapshot.index { batch.delete(row.key) }
    }
    try await database.writeBatch(batch)
  }

  /// Closes the replication log database.
  public func close() async throws { try await database.close() }

  private func readValue(_ key: [UInt8]) async throws -> Value? {
    guard let data = try await database.readKey(key) else { return nil }
    return try ValueCodec.decode(Data(data))
  }

  private func readNumber(_ key: [UInt8]) async throws -> Number? {
    guard let value = try await readValue(key) else { return nil }
    guard case .number(let number) = value else { throw KwiftDBError.corruptStorage("replication state is not numeric") }
    return number
  }

  private static func stateKey(_ name: String) -> [UInt8] {
    (try? KeyEncoder.encodeTuple([.text("r"), .text(name)])) ?? []
  }

  private static func entryKey(_ index: Number) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("r"), .text("log"), .number(index)])
  }

  private static func entryDocument(_ entry: ReplicationEntry) throws -> Document {
    var fields = [DocumentField("term", .number(entry.term)), DocumentField("batch", .bytes(Array(entry.batch))), DocumentField("notes", .list(entry.notes))]
    if let members = entry.members { fields.append(DocumentField("members", .list(try ReplicationMessage.memberValues(members)))) }
    return try Document(fields)
  }

  private static func decodeEntry(index: Number, value: Value) throws -> ReplicationEntry {
    guard case .document(let document) = value, case .number(let term)? = document["term"],
          case .bytes(let batch)? = document["batch"], case .list(let notes)? = document["notes"] else {
      throw KwiftDBError.corruptStorage("malformed persisted replication entry")
    }
    let members: [Member]?
    if case .list(let items)? = document["members"] { members = try items.map(ReplicationMessage.member) }
    else { members = nil }
    return try ReplicationEntry(index: index, term: term, batchPayload: Data(batch), notes: notes, members: members)
  }

  private static func snapshotValue(_ snapshot: ReplicationSnapshot) throws -> Value {
    .document(try Document([
      DocumentField("index", .number(snapshot.index)), DocumentField("term", .number(snapshot.term)),
      DocumentField("members", .list(try ReplicationMessage.memberValues(snapshot.members))),
    ]))
  }

  private static func decodeSnapshot(_ value: Value) throws -> ReplicationSnapshot {
    guard case .document(let document) = value, case .number(let index)? = document["index"],
          case .number(let term)? = document["term"], case .list(let values)? = document["members"] else {
      throw KwiftDBError.corruptStorage("malformed replication snapshot")
    }
    return ReplicationSnapshot(index: index, term: term, members: try values.map(ReplicationMessage.member))
  }
}
