import Foundation

/// One write-ahead log record's fixed header: payload length and CRC-32C.
internal enum LogRecord {
  static let headerBytes = 8
}

/// The append-only writer for the active write-ahead log.
///
/// The batch's four-byte payload length and four-byte checksum precede the
/// encoded payload. A write is considered acknowledged only after the complete
/// record has been synchronized to storage.
internal final class LogWriter {
  private let file: StorageFile
  private var end: UInt64

  private init(file: StorageFile, end: UInt64) {
    self.file = file
    self.end = end
  }

  /// Creates a new, empty log with a never-used file number.
  static func create(at url: URL) throws -> LogWriter {
    LogWriter(file: try StorageFile.createNew(at: url), end: 0)
  }

  /// Reopens an existing log at its current end after recovery.
  static func openForAppend(at url: URL, validByteCount: UInt64) throws -> LogWriter {
    let file = try StorageFile.openForUpdating(at: url)
    try file.truncate(to: validByteCount)
    return LogWriter(file: file, end: validByteCount)
  }

  /// Appends a checksummed record and waits for the file sync to finish.
  func append(_ batch: SequencedBatch) throws {
    let payload = batch.encodePayload()
    guard payload.count <= Int(UInt32.max) else {
      throw KwiftDBError.io("write batch exceeds the four-byte WAL length")
    }
    var record = Data()
    BinaryWriter.appendBigEndian(UInt32(payload.count), to: &record)
    BinaryWriter.appendBigEndian(CRC32C.checksum(payload), to: &record)
    record.append(payload)
    let offset = try file.append(record)
    guard offset == end else { throw KwiftDBError.io("write-ahead log end changed unexpectedly") }
    try file.synchronize()
    end += UInt64(record.count)
  }

  /// Closes the active log file.
  func close() throws { try file.close() }
}

/// Reads every acknowledged batch from a write-ahead log.
internal enum LogReader {
  /// Replays complete records in order.
  ///
  /// A truncated header, truncated payload, or checksum mismatch ends the log:
  /// that record and everything after it was not acknowledged. A complete
  /// checksum-valid record with an invalid batch is corruption and is reported.
  static func read(at url: URL) throws -> LogReadResult {
    let bytes: [UInt8]
    do { bytes = Array(try Data(contentsOf: url)) }
    catch { throw KwiftDBError.io(error.localizedDescription) }

    var offset = 0
    var validByteCount = 0
    var batches: [SequencedBatch] = []
    while offset < bytes.count {
      guard bytes.count - offset >= LogRecord.headerBytes else { break }
      let length = readWord(bytes, at: offset)
      let expected = readWord(bytes, at: offset + 4)
      offset += LogRecord.headerBytes
      guard UInt64(length) <= UInt64(bytes.count - offset) else { break }
      let end = offset + Int(length)
      let payload = Array(bytes[offset..<end])
      guard CRC32C.checksum(payload) == expected else { break }
      do { batches.append(try SequencedBatch.decodePayload(Data(payload))) }
      catch { throw KwiftDBError.corruptStorage("invalid batch in \(url.lastPathComponent): \(error.localizedDescription)") }
      offset = end
      validByteCount = end
    }
    return LogReadResult(batches: batches, validByteCount: UInt64(validByteCount))
  }

  private static func readWord(_ bytes: [UInt8], at offset: Int) -> UInt32 {
    (UInt32(bytes[offset]) << 24)
      | (UInt32(bytes[offset + 1]) << 16)
      | (UInt32(bytes[offset + 2]) << 8)
      | UInt32(bytes[offset + 3])
  }
}
