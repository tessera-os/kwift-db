import Foundation

/// The permanent lifecycle state of one durable stream.
public enum StreamState: Sendable, Equatable {
  /// The producer may append the next non-empty chunk.
  case open
  /// The producer ended the stream successfully.
  case completed
  /// The producer ended the stream with a failure reason.
  case failed(String)
}

/// One immutable text chunk and its zero-based stream sequence.
public struct StreamChunk: Sendable, Equatable {
  /// The exact sequence assigned when the chunk was committed.
  public var sequence: Number
  /// Text content for this chunk.
  public var content: String
}

/// A resumable view of a durable stream from a requested cursor.
public struct StreamRead: Sendable, Equatable {
  /// Chunks whose sequence is at or after the requested cursor.
  public var chunks: [StreamChunk]
  /// The concatenation of `chunks` in sequence order.
  public var content: String
  /// The next sequence the producer would allocate, not the next unread item.
  public var nextCursor: Number
  /// The persisted terminal or open state.
  public var state: StreamState
}

/// Durable append-only streams stored in a KwiftDB database session.
///
/// Metadata lives in the reserved `__kira_streams` document collection and
/// chunks use the reserved `__kira_stream_chunks` namespace. A chunk and its
/// metadata cursor advance are written in one database batch, so reopening a
/// database cannot expose a cursor without its corresponding content.
public actor DurableStreams {
  private let session: any DatabaseSession

  /// Creates a stream API over a live database or recorded run.
  public init(session: any DatabaseSession) { self.session = session }

  /// Creates a new open stream and returns its stable reference value.
  public func begin(id: String) async throws -> Value {
    guard !id.isEmpty else { throw KwiftDBError.table("stream id cannot be empty") }
    let key = try Self.metadataKey(id)
    guard try await session.readKey(key) == nil else {
      throw KwiftDBError.table("stream already exists: \(id)")
    }
    let metadata = try Self.metadata(next: .zero, state: .open)
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.document(metadata))), for: key)
    try await session.writeBatch(batch)
    return .stream(id)
  }

  /// Appends one non-empty chunk at exactly the stream's next sequence.
  ///
  /// The stream must exist and remain open. A duplicate, skipped, negative, or
  /// otherwise out-of-order sequence is rejected without changing the store.
  public func append(_ id: String, sequence: Number, content: String) async throws {
    guard !content.isEmpty else { throw KwiftDBError.table("stream chunks cannot be empty") }
    let (metadataKey, next, state) = try await readMetadata(id)
    guard state == .open else { throw KwiftDBError.table("stream is already terminal") }
    let supplied = try Self.unsigned(sequence)
    guard supplied == next else { throw KwiftDBError.table("stream chunk sequence is out of order") }
    let value = try Number(supplied.description)
    let chunkKey = try Self.chunkKey(id, sequence: value)
    let updated = try Self.metadata(next: next.incremented(), state: .open)
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.text(content))), for: chunkKey)
    batch.put(Array(try ValueCodec.encode(.document(updated))), for: metadataKey)
    try await session.writeBatch(batch)
  }

  /// Permanently marks an open stream as complete.
  public func complete(_ id: String) async throws {
    try await finish(id, state: .completed)
  }

  /// Permanently marks an open stream as failed while retaining partial text.
  public func fail(_ id: String, reason: String) async throws {
    try await finish(id, state: .failed(reason))
  }

  /// Reads every chunk committed so far, including partial terminal content.
  public func read(_ id: String) async throws -> StreamRead {
    try await readSince(id, cursor: .zero)
  }

  /// Reads chunks at or after `cursor` for lossless reconnect and resumption.
  public func readSince(_ id: String, cursor: Number) async throws -> StreamRead {
    let (_, next, state) = try await readMetadata(id)
    let requested = try Self.unsigned(cursor)
    guard requested <= next else { throw KwiftDBError.table("stream cursor is beyond its next sequence") }
    let prefix = try Self.chunkPrefix(id)
    let start = prefix + (try KeyEncoder.encode(.number(cursor)))
    let end = try DocumentCollection.upperBound(after: prefix)
    var chunks: [StreamChunk] = []
    for row in try await session.scanKeys(from: start, to: end) {
      let tuple = try KeyDecoder.decodeTuple(row.key)
      guard tuple.count == 4, case .number(let sequence) = tuple[3],
            case .text(let content) = try ValueCodec.decode(Data(row.value)) else {
        throw KwiftDBError.corruptStorage("malformed durable stream chunk")
      }
      chunks.append(StreamChunk(sequence: sequence, content: content))
    }
    chunks.sort { $0.sequence < $1.sequence }
    return StreamRead(
      chunks: chunks,
      content: chunks.map(\.content).joined(),
      nextCursor: try Number(next.description),
      state: state
    )
  }

  private func finish(_ id: String, state newState: StreamState) async throws {
    let (key, next, state) = try await readMetadata(id)
    guard state == .open else { throw KwiftDBError.table("stream is already terminal") }
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.document(Self.metadata(next: next, state: newState)))), for: key)
    try await session.writeBatch(batch)
  }

  private func readMetadata(_ id: String) async throws -> (key: [UInt8], next: DatabaseUInt, state: StreamState) {
    let key = try Self.metadataKey(id)
    guard let bytes = try await session.readKey(key),
          case .document(let document) = try ValueCodec.decode(Data(bytes)),
          case .number(let number)? = document["next"],
          case .text(let stateName)? = document["state"] else {
      throw KwiftDBError.table("unknown stream: \(id)")
    }
    let state: StreamState
    switch stateName {
    case "open": state = .open
    case "completed": state = .completed
    case "failed":
      guard case .text(let reason)? = document["reason"] else {
        throw KwiftDBError.corruptStorage("failed stream has no reason")
      }
      state = .failed(reason)
    default: throw KwiftDBError.corruptStorage("unknown durable stream state")
    }
    return (key, try Self.unsigned(number), state)
  }

  private static func metadata(next: DatabaseUInt, state: StreamState) throws -> Document {
    var fields = [
      DocumentField("next", .number(try Number(next.description))),
    ]
    switch state {
    case .open: fields.append(DocumentField("state", .text("open")))
    case .completed: fields.append(DocumentField("state", .text("completed")))
    case .failed(let reason):
      fields.append(DocumentField("state", .text("failed")))
      fields.append(DocumentField("reason", .text(reason)))
    }
    return try Document(fields)
  }

  private static func unsigned(_ number: Number) throws -> DatabaseUInt {
    guard let text = number.unsignedIntegerText else {
      throw KwiftDBError.table("stream sequence must be a non-negative integer")
    }
    return try DatabaseUInt(text)
  }

  private static func metadataKey(_ id: String) throws -> [UInt8] {
    try DocumentCollection.storageKey(collection: "__kira_streams", key: .text(id))
  }

  private static func chunkPrefix(_ id: String) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("d"), .text("__kira_stream_chunks"), .text(id)])
  }

  private static func chunkKey(_ id: String, sequence: Number) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("d"), .text("__kira_stream_chunks"), .text(id), .number(sequence)])
  }
}
