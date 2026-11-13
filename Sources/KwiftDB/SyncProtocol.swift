import Foundation

/// One mutation result returned by a sync server.
public enum SyncMutationOutcome: Sendable, Equatable {
  /// The mutation ran and returned its value.
  case value(Value)
  /// The mutation ran but failed without committing writes.
  case failure(String)
  /// The server had already applied this mutation number in an earlier session.
  case skipped
}

/// A query id paired with its first complete held-range transition.
public struct SyncReady: Sendable, Equatable {
  /// The replica's subscription id.
  public var query: Number
  /// Creates a ready notification.
  public init(query: Number) { self.query = query }
}

/// One state change that a replica applies atomically.
public enum SyncChange: Sendable, Equatable {
  /// Writes or removes one key; `nil` means delete.
  case write(key: [UInt8], value: [UInt8]?)
  /// Replaces the complete contents of a newly held range.
  case hold(range: KeyRange, pairs: [KeyValue])
  /// Releases a range and removes every key it covered from the replica.
  case release(KeyRange)
}

/// A framed message exchanged by sync clients and servers.
public enum SyncMessage: Sendable {
  /// Starts a session; client zero requests a new durable id.
  case hello(client: Number, credentials: Value)
  /// Subscribes to one named query function.
  case subscribe(query: Number, name: String, arguments: Value)
  /// Removes a subscription.
  case unsubscribe(query: Number)
  /// Requests one ordered mutation.
  case mutate(number: Number, name: String, arguments: Value)
  /// A client's sign of life on a quiet session. The server answers nothing;
  /// the transport's acknowledgement keeps both ends' idle timers fresh.
  case heartbeat
  /// Confirms an authenticated session and its durable client id.
  case welcome(client: Number, identity: Value)
  /// Refuses the session and ends it.
  case refused(reason: String)
  /// Carries an atomic replica transition.
  case transition(version: Number, applied: Number, changes: [SyncChange], ready: [SyncReady], results: [(Number, SyncMutationOutcome)])

  /// Encodes this message as the exact value document described by the sync protocol.
  public func encodedValue() throws -> Data {
    let fields: [DocumentField]
    switch self {
    case .hello(let client, let credentials):
      fields = [DocumentField("type", .text("hello")), DocumentField("client", .number(client)), DocumentField("credentials", credentials)]
    case .subscribe(let query, let name, let arguments):
      fields = [DocumentField("type", .text("subscribe")), DocumentField("query", .number(query)), DocumentField("name", .text(name)), DocumentField("arguments", arguments)]
    case .unsubscribe(let query):
      fields = [DocumentField("type", .text("unsubscribe")), DocumentField("query", .number(query))]
    case .mutate(let number, let name, let arguments):
      fields = [DocumentField("type", .text("mutate")), DocumentField("number", .number(number)), DocumentField("name", .text(name)), DocumentField("arguments", arguments)]
    case .heartbeat:
      fields = [DocumentField("type", .text("heartbeat"))]
    case .welcome(let client, let identity):
      fields = [DocumentField("type", .text("welcome")), DocumentField("client", .number(client)), DocumentField("identity", identity)]
    case .refused(let reason):
      fields = [DocumentField("type", .text("refused")), DocumentField("reason", .text(reason))]
    case .transition(let version, let applied, let changes, let ready, let results):
      fields = [
        DocumentField("type", .text("transition")),
        DocumentField("version", .number(version)),
        DocumentField("applied", .number(applied)),
        DocumentField("changes", .list(try changes.map(Self.encodeChange))),
        DocumentField("ready", .list(ready.map { .number($0.query) })),
        DocumentField("results", .list(try results.map(Self.encodeResult))),
      ]
    }
    return try ValueCodec.encode(.document(Document(fields: fields)))
  }

  /// Decodes a message document and validates its required fields and order-independent names.
  public static func decodeValue(_ bytes: Data) throws -> SyncMessage {
    guard case .document(let document) = try ValueCodec.decode(bytes),
          case .text(let type)? = document["type"] else {
      throw KwiftDBError.invalidEncoding("sync message is not a typed document")
    }
    let requiredOrder: [String]
    switch type {
    case "hello": requiredOrder = ["type", "client", "credentials"]
    case "subscribe": requiredOrder = ["type", "query", "name", "arguments"]
    case "unsubscribe": requiredOrder = ["type", "query"]
    case "mutate": requiredOrder = ["type", "number", "name", "arguments"]
    case "heartbeat": requiredOrder = ["type"]
    case "welcome": requiredOrder = ["type", "client", "identity"]
    case "refused": requiredOrder = ["type", "reason"]
    case "transition": requiredOrder = ["type", "version", "applied", "changes", "ready", "results"]
    default: throw KwiftDBError.invalidEncoding("unknown sync message type \(type)")
    }
    guard document.fields.map(\.name) == requiredOrder else {
      throw KwiftDBError.invalidEncoding("sync message fields are missing, extra, or out of order")
    }
    switch type {
    case "hello": return .hello(client: try number("client", in: document), credentials: try required("credentials", in: document))
    case "subscribe": return .subscribe(query: try number("query", in: document), name: try text("name", in: document), arguments: try required("arguments", in: document))
    case "unsubscribe": return .unsubscribe(query: try number("query", in: document))
    case "heartbeat": return .heartbeat
    case "mutate": return .mutate(number: try number("number", in: document), name: try text("name", in: document), arguments: try required("arguments", in: document))
    case "welcome": return .welcome(client: try number("client", in: document), identity: try required("identity", in: document))
    case "refused": return .refused(reason: try text("reason", in: document))
    case "transition":
      guard case .list(let rawChanges)? = document["changes"],
            case .list(let rawReady)? = document["ready"],
            case .list(let rawResults)? = document["results"] else {
        throw KwiftDBError.invalidEncoding("transition is missing a list field")
      }
      let changes = try rawChanges.map(decodeChange)
      let ready = try rawReady.map { value -> SyncReady in
        guard case .number(let query) = value else { throw KwiftDBError.invalidEncoding("malformed ready item") }
        return SyncReady(query: query)
      }
      let results = try rawResults.map { value -> (Number, SyncMutationOutcome) in
        guard case .document(let record) = value, case .number(let n)? = record["number"] else {
          throw KwiftDBError.invalidEncoding("malformed mutation result")
        }
        if let value = record["value"], record.fields.map(\.name) == ["number", "value"] { return (n, .value(value)) }
        if case .text(let reason)? = record["failure"], record.fields.map(\.name) == ["number", "failure"] { return (n, .failure(reason)) }
        if case .bool(true)? = record["skipped"], record.fields.map(\.name) == ["number", "skipped"] { return (n, .skipped) }
        throw KwiftDBError.invalidEncoding("mutation result has no outcome")
      }
      return .transition(
        version: try number("version", in: document),
        applied: try number("applied", in: document),
        changes: changes,
        ready: ready,
        results: results
      )
    default: throw KwiftDBError.invalidEncoding("unknown sync message type \(type)")
    }
  }

  private static func encodeChange(_ change: SyncChange) throws -> Value {
    switch change {
    case .write(let key, let value):
      return .document(try Document([
        DocumentField("type", .text("write")), DocumentField("key", .bytes(key)),
        DocumentField("value", value.map { .bytes($0) } ?? .null),
      ]))
    case .hold(let range, let pairs):
      let values = try pairs.map { try Document([DocumentField("key", .bytes($0.key)), DocumentField("value", .bytes($0.value))]) }
      return .document(try Document([
        DocumentField("type", .text("hold")), DocumentField("start", .bytes(range.start)),
        DocumentField("end", .bytes(range.end)), DocumentField("pairs", .list(values.map(Value.document))),
      ]))
    case .release(let range):
      return .document(try Document([
        DocumentField("type", .text("release")), DocumentField("start", .bytes(range.start)),
        DocumentField("end", .bytes(range.end)),
      ]))
    }
  }

  private static func decodeChange(_ value: Value) throws -> SyncChange {
    guard case .document(let record) = value, case .text(let type)? = record["type"] else {
      throw KwiftDBError.invalidEncoding("malformed transition change")
    }
    switch type {
    case "write":
      guard record.fields.map(\.name) == ["type", "key", "value"] else { throw KwiftDBError.invalidEncoding("write change fields are out of order") }
      guard case .bytes(let key)? = record["key"] else { throw KwiftDBError.invalidEncoding("write has no key") }
      let bytes: [UInt8]?
      switch record["value"] {
      case .bytes(let value)?: bytes = value
      case .null?: bytes = nil
      default: throw KwiftDBError.invalidEncoding("write has an invalid value")
      }
      return .write(key: key, value: bytes)
    case "hold":
      guard record.fields.map(\.name) == ["type", "start", "end", "pairs"] else { throw KwiftDBError.invalidEncoding("hold change fields are out of order") }
      guard case .bytes(let start)? = record["start"], case .bytes(let end)? = record["end"],
            case .list(let rawPairs)? = record["pairs"] else {
        throw KwiftDBError.invalidEncoding("hold is missing a range or pair list")
      }
      let pairs = try rawPairs.map { value -> KeyValue in
        guard case .document(let pair) = value, case .bytes(let key)? = pair["key"],
              case .bytes(let bytes)? = pair["value"] else {
          throw KwiftDBError.invalidEncoding("malformed held pair")
        }
        return KeyValue(key: key, value: bytes)
      }
      return .hold(range: KeyRange(start: start, end: end), pairs: pairs)
    case "release":
      guard record.fields.map(\.name) == ["type", "start", "end"] else { throw KwiftDBError.invalidEncoding("release change fields are out of order") }
      guard case .bytes(let start)? = record["start"], case .bytes(let end)? = record["end"] else {
        throw KwiftDBError.invalidEncoding("release has no range")
      }
      return .release(KeyRange(start: start, end: end))
    default: throw KwiftDBError.invalidEncoding("unknown transition change \(type)")
    }
  }

  private static func encodeResult(_ result: (Number, SyncMutationOutcome)) throws -> Value {
    var fields = [DocumentField("number", .number(result.0))]
    switch result.1 {
    case .value(let value): fields.append(DocumentField("value", value))
    case .failure(let reason): fields.append(DocumentField("failure", .text(reason)))
    case .skipped: fields.append(DocumentField("skipped", .bool(true)))
    }
    return .document(try Document(fields))
  }

  private static func required(_ name: String, in document: Document) throws -> Value {
    guard let value = document[name] else { throw KwiftDBError.invalidEncoding("sync message is missing \(name)") }
    return value
  }

  private static func number(_ name: String, in document: Document) throws -> Number {
    guard case .number(let number)? = document[name] else { throw KwiftDBError.invalidEncoding("sync field \(name) is not a number") }
    return number
  }

  private static func text(_ name: String, in document: Document) throws -> String {
    guard case .text(let text)? = document[name] else { throw KwiftDBError.invalidEncoding("sync field \(name) is not text") }
    return text
  }
}

/// Encodes protocol messages as length-prefixed value frames.
public enum SyncFrameCodec {
  /// Creates one varint-length-prefixed message frame.
  public static func encode(_ message: SyncMessage) throws -> Data {
    let payload = try message.encodedValue()
    var frame = Data(DatabaseUInt(UInt64(payload.count)).varintBytes())
    frame.append(payload)
    return frame
  }
}

/// Incrementally extracts complete protocol messages from a bidirectional byte stream.
public struct SyncFrameReader: Sendable {
  private var splitter = FrameSplitter()

  /// Creates an empty stream reader.
  public init() {}

  /// Adds network bytes and returns all complete frames now buffered.
  public mutating func append(_ bytes: [UInt8]) throws -> [SyncMessage] {
    try splitter.append(bytes).map { try SyncMessage.decodeValue($0) }
  }
}
