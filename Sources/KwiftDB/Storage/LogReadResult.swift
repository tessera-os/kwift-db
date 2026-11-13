/// Recovered write-ahead log batches and the last fully checksummed byte offset.
internal struct LogReadResult: Sendable {
  /// Every complete, acknowledged batch in the log.
  var batches: [SequencedBatch]
  /// The offset after the final valid record; any later bytes are a torn tail.
  var validByteCount: UInt64
}
