import Foundation

/// Shared binary readers and writers for the log and sorted-table formats.
///
/// This layer owns framing details such as unsigned varints and fixed
/// big-endian words. Higher storage layers decide which records and payloads
/// those bytes represent.
internal enum BinaryWriter {
  /// Appends an arbitrary-width unsigned integer in base-128 varint form.
  static func append(_ value: DatabaseUInt, to data: inout Data) {
    data.append(contentsOf: value.varintBytes())
  }

  /// Appends a machine-sized non-negative length as a varint.
  static func appendLength(_ length: Int, to data: inout Data) {
    precondition(length >= 0)
    append(DatabaseUInt(UInt64(length)), to: &data)
  }

  /// Appends bytes preceded by their varint length.
  static func appendLengthPrefixed(_ bytes: [UInt8], to data: inout Data) {
    appendLength(bytes.count, to: &data)
    data.append(contentsOf: bytes)
  }

  /// Appends a four-byte unsigned word in network byte order.
  static func appendBigEndian(_ value: UInt32, to data: inout Data) {
    data.append(UInt8((value >> 24) & 0xff))
    data.append(UInt8((value >> 16) & 0xff))
    data.append(UInt8((value >> 8) & 0xff))
    data.append(UInt8(value & 0xff))
  }
}

/// A cursor that validates every read against one record's byte boundary.
internal struct BinaryReader {
  private let bytes: [UInt8]
  private(set) var offset = 0

  /// Creates a reader over one complete record or block.
  init(_ data: Data) { bytes = Array(data) }

  /// Whether every input byte has been consumed.
  var isAtEnd: Bool { offset == bytes.count }

  /// Reads one byte or reports a truncated record.
  mutating func readByte() throws -> UInt8 {
    guard offset < bytes.count else { throw KwiftDBError.invalidEncoding("unexpected end of record") }
    defer { offset += 1 }
    return bytes[offset]
  }

  /// Reads an arbitrary-width unsigned varint.
  mutating func readVarint() throws -> DatabaseUInt {
    try DatabaseUInt.readVarint(from: bytes, offset: &offset)
  }

  /// Reads a varint length after checking that it fits the current process.
  mutating func readLength() throws -> Int {
    let value = try readVarint()
    guard let length = value.intValue else {
      throw KwiftDBError.invalidEncoding("record length is too large")
    }
    return length
  }

  /// Reads exactly `count` bytes and advances the cursor.
  mutating func readBytes(count: Int) throws -> [UInt8] {
    guard count >= 0, count <= bytes.count - offset else {
      throw KwiftDBError.invalidEncoding("truncated length-prefixed field")
    }
    defer { offset += count }
    return Array(bytes[offset..<(offset + count)])
  }

  /// Reads a byte array preceded by its varint length.
  mutating func readLengthPrefixedBytes() throws -> [UInt8] {
    try readBytes(count: readLength())
  }

  /// Reads a four-byte big-endian unsigned word.
  mutating func readBigEndianUInt32() throws -> UInt32 {
    let bytes = try readBytes(count: 4)
    return (UInt32(bytes[0]) << 24)
      | (UInt32(bytes[1]) << 16)
      | (UInt32(bytes[2]) << 8)
      | UInt32(bytes[3])
  }
}
