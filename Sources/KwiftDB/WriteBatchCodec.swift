import Foundation

/// The stable batch payload shared by the WAL and replication log.
public enum WriteBatchCodec {
  /// Encodes operations with sequence zero, as required by replicated entries.
  public static func encode(_ batch: WriteBatch) -> Data {
    SequencedBatch(sequence: .zero, operations: batch.operations).encodePayload()
  }

  /// Decodes a batch payload and requires the replication sequence to be zero.
  public static func decode(_ payload: Data) throws -> WriteBatch {
    let decoded = try SequencedBatch.decodePayload(payload)
    guard decoded.sequence.isZero else {
      throw KwiftDBError.invalidEncoding("replicated batch sequence must be zero")
    }
    var batch = WriteBatch()
    for operation in decoded.operations {
      switch operation {
      case .put(let key, let value): batch.put(value, for: key)
      case .delete(let key): batch.delete(key)
      }
    }
    return batch
  }
}
