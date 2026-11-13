import Foundation

/// A named collection of documents stored under the database's `d` keyspace.
///
/// Collection keys are order-preserving tuples of the `d` namespace, the
/// collection name, and a caller-provided orderable `Value`. Document payloads
/// use `ValueCodec`, so nested values retain their exact number and byte types.
/// The session can be either a live database or a recorded run; the latter
/// makes collection operations participate in read tracking and atomic writes.
public struct DocumentCollection: Sendable {
  /// The logical collection name.
  public let name: String
  private let session: any DatabaseSession

  internal init(session: any DatabaseSession, name: String) {
    self.session = session
    self.name = name
  }

  /// Reads and decodes a document for an orderable key.
  public func document(for key: Value) async throws -> Document? {
    let storageKey = try Self.storageKey(collection: name, key: key)
    guard let bytes = try await session.readKey(storageKey) else { return nil }
    guard case .document(let document) = try ValueCodec.decode(Data(bytes)) else {
      throw KwiftDBError.corruptStorage("document collection contains a non-document value")
    }
    return document
  }

  /// Writes or replaces a document under an orderable key.
  public func put(_ document: Document, for key: Value) async throws {
    let storageKey = try Self.storageKey(collection: name, key: key)
    let encoded = try ValueCodec.encode(.document(document))
    var batch = WriteBatch()
    batch.put(Array(encoded), for: storageKey)
    try await session.writeBatch(batch)
  }

  /// Deletes a document under an orderable key.
  public func delete(_ key: Value) async throws {
    let storageKey = try Self.storageKey(collection: name, key: key)
    var batch = WriteBatch()
    batch.delete(storageKey)
    try await session.writeBatch(batch)
  }

  /// Reads every document in key order, returning the stored key with it.
  public func allDocuments() async throws -> [DocumentEntry] {
    let prefix = try Self.collectionPrefix(name)
    let end = try Self.upperBound(after: prefix)
    let rows = try await session.scanKeys(from: prefix, to: end)
    return try rows.map { row in
      guard case .document(let document) = try ValueCodec.decode(Data(row.value)) else {
        throw KwiftDBError.corruptStorage("document collection contains a non-document value")
      }
      let keyBytes = Array(row.key.dropFirst(prefix.count))
      return DocumentEntry(key: try KeyDecoder.decode(keyBytes), document: document)
    }
  }

  /// Encodes a complete key for one collection document.
  internal static func storageKey(collection: String, key: Value) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("d"), .text(collection), key])
  }

  /// Encodes the fixed key prefix shared by all documents in a collection.
  internal static func collectionPrefix(_ name: String) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("d"), .text(name)])
  }

  /// Computes the smallest byte string strictly above every string with prefix.
  internal static func upperBound(after prefix: [UInt8]) throws -> [UInt8] {
    var end = prefix
    for index in end.indices.reversed() where end[index] != .max {
      end[index] += 1
      end.removeSubrange((index + 1)..<end.count)
      return end
    }
    throw KwiftDBError.unsupportedKeyValue("key prefix has no finite upper bound")
  }
}

/// One decoded document and its orderable collection key.
public struct DocumentEntry: Sendable, Equatable {
  /// The key supplied when the document was stored.
  public var key: Value
  /// The stored document.
  public var document: Document

  /// Creates a scanned document entry.
  public init(key: Value, document: Document) {
    self.key = key
    self.document = document
  }
}
