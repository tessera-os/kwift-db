import Foundation

/// Decodes the order-preserving tuple format produced by `KeyEncoder`.
///
/// Key decoding is useful for document scans and table cursors. The decoder
/// accepts only complete, well-formed components; malformed escapes, number
/// bodies, UTF-8 text, and trailing fragments are rejected instead of silently
/// returning a valid prefix.
public enum KeyDecoder {
  /// Decodes exactly one orderable value.
  public static func decode(_ bytes: [UInt8]) throws -> Value {
    let values = try decodeTuple(bytes)
    guard values.count == 1 else {
      throw KwiftDBError.invalidEncoding("expected one key component")
    }
    return values[0]
  }

  /// Decodes every component in one concatenated tuple key.
  public static func decodeTuple(_ bytes: [UInt8]) throws -> [Value] {
    var cursor = KeyCursor(bytes)
    var values: [Value] = []
    while !cursor.isAtEnd { values.append(try cursor.readValue()) }
    return values
  }
}

private struct KeyCursor {
  private let bytes: [UInt8]
  private var offset = 0

  init(_ bytes: [UInt8]) { self.bytes = bytes }
  var isAtEnd: Bool { offset == bytes.count }

  mutating func readValue() throws -> Value {
    let tag = try readByte()
    switch tag {
    case 1: return .null
    case 2: return .bool(false)
    case 3: return .bool(true)
    case 16: return .number(try readNumber(negative: true))
    case 17: return .number(.zero)
    case 18: return .number(try readNumber(negative: false))
    case 32:
      guard let text = String(bytes: try readEscaped(), encoding: .utf8) else {
        throw KwiftDBError.invalidEncoding("key text is not valid UTF-8")
      }
      return .text(text)
    case 48: return .bytes(try readEscaped())
    default: throw KwiftDBError.invalidEncoding("unknown key tag \(tag)")
    }
  }

  private mutating func readNumber(negative: Bool) throws -> Number {
    let terminator: UInt8 = negative ? 255 : 0
    var body: [UInt8] = []
    for _ in 0..<2 {
      let byte = try readByte()
      body.append(negative ? 255 - byte : byte)
    }
    while true {
      let byte = try readByte()
      if byte == terminator { break }
      body.append(negative ? 255 - byte : byte)
    }
    guard body.count >= 3 else { throw KwiftDBError.invalidEncoding("short sortable number") }
    let biased = UInt16(body[0]) << 8 | UInt16(body[1])
    let exponent = Int(biased) - 32_768
    let encodedDigits = body.dropFirst(2)
    guard !encodedDigits.isEmpty, encodedDigits.allSatisfy({ 1...10 ~= $0 }) else {
      throw KwiftDBError.invalidEncoding("invalid sortable number digits")
    }
    let digits = String(decoding: encodedDigits.map { $0 - 1 + 48 }, as: UTF8.self)
    let mantissa = digits.count == 1 ? digits : String(digits.prefix(1)) + "." + String(digits.dropFirst())
    return try Number("\(negative ? "-" : "")\(mantissa)e\(exponent)")
  }

  private mutating func readEscaped() throws -> [UInt8] {
    var result: [UInt8] = []
    while true {
      let byte = try readByte()
      if byte != 0 {
        result.append(byte)
        continue
      }
      switch try readByte() {
      case 1: return result
      case 255: result.append(0)
      default: throw KwiftDBError.invalidEncoding("invalid zero-byte escape in key")
      }
    }
  }

  private mutating func readByte() throws -> UInt8 {
    guard offset < bytes.count else { throw KwiftDBError.invalidEncoding("truncated key") }
    defer { offset += 1 }
    return bytes[offset]
  }
}
