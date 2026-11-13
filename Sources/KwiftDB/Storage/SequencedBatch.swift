import Foundation

/// A committed write batch after the store assigns its sequence number.
///
/// The payload format is shared by the write-ahead log and the replication
/// specification: sequence varint, operation count, then ordered put/delete
/// records with length-prefixed keys and values.
internal struct SequencedBatch: Sendable, Equatable {
  /// The sequence assigned to every operation in the batch.
  var sequence: DatabaseUInt
  /// Ordered operations committed at that sequence.
  var operations: [WriteOperation]

  /// Encodes this batch payload without the log's length and checksum header.
  func encodePayload() -> Data {
    var data = Data()
    BinaryWriter.append(sequence, to: &data)
    BinaryWriter.appendLength(operations.count, to: &data)
    for operation in operations {
      switch operation {
      case .put(let key, let value):
        data.append(1)
        BinaryWriter.appendLengthPrefixed(key, to: &data)
        BinaryWriter.appendLengthPrefixed(value, to: &data)
      case .delete(let key):
        data.append(2)
        BinaryWriter.appendLengthPrefixed(key, to: &data)
      }
    }
    return data
  }

  /// Decodes one complete batch payload.
  ///
  /// - Parameter data: The payload bytes after the outer WAL record header.
  /// - Throws: `KwiftDBError.invalidEncoding` for an unknown operation, a
  ///   truncated field, or trailing payload bytes.
  static func decodePayload(_ data: Data) throws -> SequencedBatch {
    var reader = BinaryReader(data)
    let sequence = try reader.readVarint()
    let count = try reader.readLength()
    var operations: [WriteOperation] = []
    operations.reserveCapacity(count)
    for _ in 0..<count {
      switch try reader.readByte() {
      case 1:
        let key = try reader.readLengthPrefixedBytes()
        let value = try reader.readLengthPrefixedBytes()
        operations.append(.put(key: key, value: value))
      case 2:
        operations.append(.delete(key: try reader.readLengthPrefixedBytes()))
      case let kind:
        throw KwiftDBError.invalidEncoding("unknown write kind \(kind)")
      }
    }
    guard reader.isAtEnd else { throw KwiftDBError.invalidEncoding("trailing batch bytes") }
    return SequencedBatch(sequence: sequence, operations: operations)
  }
}
