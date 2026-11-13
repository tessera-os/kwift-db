import Foundation

/// Encodes and decodes the stable binary representation of `Value`.
///
/// Every value begins with one tag byte. Lengths and collection counts use an
/// unsigned base-128 varint; strings and byte arrays then carry their bytes.
/// The tags are `0` null, `1` false, `2` true, `3` number, `4` text, `5` bytes,
/// `6` list, `7` document, and `8` stream. This codec is intentionally
/// independent of JSON so values round-trip without changing their types.
public enum ValueCodec {
  /// Encodes one value with no outer length prefix.
  ///
  /// A caller writing a value into a log or table supplies the surrounding
  /// record framing required by that storage format.
  ///
  /// - Parameter value: The value to encode.
  /// - Returns: The tag and payload bytes for that value.
  /// - Throws: A nested document validation error if the value is invalid.
  public static func encode(_ value: Value) throws -> Data {
    var output = Data()
    try append(value, to: &output)
    return output
  }

  /// Decodes exactly one value and rejects trailing bytes.
  ///
  /// Rejecting trailing bytes prevents one malformed value from being
  /// silently treated as a valid prefix of a larger record.
  ///
  /// - Parameter data: The encoded value bytes.
  /// - Returns: The decoded value.
  /// - Throws: `KwiftDBError.invalidEncoding` for truncation, invalid UTF-8,
  ///   an unknown tag, an overflowing length, or trailing data.
  public static func decode(_ data: Data) throws -> Value {
    var reader = ValueReader(data: data)
    let value = try reader.readValue()
    guard reader.isAtEnd else { throw KwiftDBError.invalidEncoding("trailing bytes") }
    return value
  }

  /// Appends an unsigned integer in seven-bit, low-group-first form.
  ///
  /// Every byte except the final byte carries its continuation bit. Lengths
  /// decoded from a physical Swift `Data` buffer are limited to `UInt64`,
  /// while logical database sequence numbers use their own arbitrary-width
  /// representation in the storage layer.
  internal static func appendVarint(_ value: UInt64, to output: inout Data) {
    var remainder = value
    while remainder >= 128 {
      output.append(UInt8(remainder & 0x7f) | 0x80)
      remainder >>= 7
    }
    output.append(UInt8(remainder))
  }

  /// Appends a byte array preceded by its encoded byte count.
  internal static func appendLengthPrefixed(_ bytes: Data, to output: inout Data) {
    appendVarint(UInt64(bytes.count), to: &output)
    output.append(bytes)
  }

  /// Writes a value recursively using its assigned one-byte type tag.
  private static func append(_ value: Value, to output: inout Data) throws {
    switch value {
    case .null:
      output.append(0)
    case .bool(false):
      output.append(1)
    case .bool(true):
      output.append(2)
    case .number(let number):
      output.append(3)
      appendLengthPrefixed(Data(number.description.utf8), to: &output)
    case .text(let text):
      output.append(4)
      appendLengthPrefixed(Data(text.utf8), to: &output)
    case .bytes(let bytes):
      output.append(5)
      appendLengthPrefixed(Data(bytes), to: &output)
    case .list(let values):
      output.append(6)
      appendVarint(UInt64(values.count), to: &output)
      for value in values { try append(value, to: &output) }
    case .document(let document):
      output.append(7)
      appendVarint(UInt64(document.count), to: &output)
      for field in document.fields {
        appendLengthPrefixed(Data(field.name.utf8), to: &output)
        try append(field.value, to: &output)
      }
    case .stream(let id):
      output.append(8)
      appendLengthPrefixed(Data(id.utf8), to: &output)
    }
  }
}

/// A bounds-checked reader for a single value payload.
///
/// All cursor movement is centralized here so recursive values cannot read
/// beyond the record being decoded and malformed lengths fail before slicing.
private struct ValueReader {
  private let data: Data
  private var offset = 0

  init(data: Data) { self.data = data }

  var isAtEnd: Bool { offset == data.count }

  /// Reads a tag and exactly the payload shape assigned to it by the spec.
  mutating func readValue() throws -> Value {
    let tag = try readByte()
    switch tag {
    case 0: return .null
    case 1: return .bool(false)
    case 2: return .bool(true)
    case 3:
      let text = try readString()
      return .number(try Number(text))
    case 4: return .text(try readString())
    case 5: return .bytes(Array(try readBytes()))
    case 6:
      let count = try readCount()
      var values: [Value] = []
      values.reserveCapacity(count)
      for _ in 0..<count { values.append(try readValue()) }
      return .list(values)
    case 7:
      let count = try readCount()
      var fields: [DocumentField] = []
      fields.reserveCapacity(count)
      for _ in 0..<count {
        fields.append(DocumentField(try readString(), try readValue()))
      }
      return .document(try Document(fields: fields))
    case 8: return .stream(try readString())
    default: throw KwiftDBError.invalidEncoding("unknown value tag \(tag)")
    }
  }

  /// Reads one byte while advancing the cursor.
  private mutating func readByte() throws -> UInt8 {
    guard offset < data.count else { throw KwiftDBError.invalidEncoding("unexpected end of input") }
    defer { offset += 1 }
    return data[offset]
  }

  /// Reads a UInt64 varint and rejects overflow and unterminated encodings.
  private mutating func readVarint() throws -> UInt64 {
    var result: UInt64 = 0
    var shift: UInt64 = 0
    for _ in 0..<10 {
      let byte = try readByte()
      let part = UInt64(byte & 0x7f)
      guard shift < 64, part <= (UInt64.max >> shift) else {
        throw KwiftDBError.invalidEncoding("variable-length number overflow")
      }
      result |= part << shift
      if byte & 0x80 == 0 { return result }
      shift += 7
    }
    throw KwiftDBError.invalidEncoding("unterminated variable-length number")
  }

  /// Converts a collection length only after checking it fits this process.
  private mutating func readCount() throws -> Int {
    let length = try readVarint()
    guard length <= UInt64(Int.max) else { throw KwiftDBError.invalidEncoding("collection is too large") }
    return Int(length)
  }

  /// Reads a length-prefixed slice without allowing integer overflow or
  /// reading past the end of the containing record.
  private mutating func readBytes() throws -> Data {
    let length = try readVarint()
    guard length <= UInt64(Int.max) else { throw KwiftDBError.invalidEncoding("field is too large") }
    let count = Int(length)
    guard count <= data.count - offset else { throw KwiftDBError.invalidEncoding("truncated field") }
    defer { offset += count }
    return data.subdata(in: offset..<(offset + count))
  }

  /// Reads a length-prefixed string and requires well-formed UTF-8.
  private mutating func readString() throws -> String {
    let bytes = try readBytes()
    guard let value = String(data: bytes, encoding: .utf8) else {
      throw KwiftDBError.invalidEncoding("field is not valid UTF-8")
    }
    return value
  }
}
