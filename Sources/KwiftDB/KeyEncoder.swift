import Foundation

/// Produces order-preserving byte keys from the orderable `Value` cases.
///
/// Comparing the returned byte arrays lexicographically gives the same order
/// as comparing their values. Tuple encoding is concatenation: the first value
/// decides order before any later value, which makes these keys usable for
/// composite table keys and index prefixes.
public enum KeyEncoder {
  /// Encodes one orderable value as a complete key component.
  ///
  /// - Parameter value: A null, Boolean, number, text, or byte value.
  /// - Returns: The value's sortable byte representation.
  /// - Throws: `KwiftDBError.unsupportedKeyValue` when the value is a list,
  ///   document, stream, or a number outside the format's exponent range.
  public static func encode(_ value: Value) throws -> [UInt8] {
    try encodeTuple([value])
  }

  /// Encodes values in tuple order by concatenating their key components.
  ///
  /// Text and byte components escape zero bytes and end with a two-byte
  /// terminator, so a component prefix sorts before a longer component without
  /// making the next tuple element ambiguous.
  ///
  /// - Parameter values: The ordered key components.
  /// - Returns: Their concatenated sortable bytes.
  /// - Throws: `KwiftDBError.unsupportedKeyValue` for a non-orderable value.
  public static func encodeTuple(_ values: [Value]) throws -> [UInt8] {
    var output: [UInt8] = []
    for value in values { try append(value, to: &output) }
    return output
  }

  /// Appends the type tag and body for one supported value.
  private static func append(_ value: Value, to output: inout [UInt8]) throws {
    switch value {
    case .null:
      output.append(1)
    case .bool(false):
      output.append(2)
    case .bool(true):
      output.append(3)
    case .number(let number):
      try append(number, to: &output)
    case .text(let text):
      output.append(32)
      appendEscaped(text.utf8, to: &output)
    case .bytes(let bytes):
      output.append(48)
      appendEscaped(bytes, to: &output)
    case .list:
      throw KwiftDBError.unsupportedKeyValue("lists are not orderable")
    case .document:
      throw KwiftDBError.unsupportedKeyValue("documents are not orderable")
    case .stream:
      throw KwiftDBError.unsupportedKeyValue("streams are not orderable")
    }
  }

  /// Writes numeric sign, exponent, and digits in sortable order.
  ///
  /// The positive body biases the scientific exponent by 32768, emits it as
  /// two big-endian bytes, then emits each decimal digit plus one and a zero
  /// terminator. Negative magnitudes complement every body byte, reversing
  /// their order so larger magnitudes sort first among negative values.
  private static func append(_ number: Number, to output: inout [UInt8]) throws {
    if number.isZero {
      output.append(17)
      return
    }

    let magnitude = number.keyMagnitude
    guard let exponent = magnitude.exponent, (-32_768...32_767).contains(exponent) else {
      throw KwiftDBError.unsupportedKeyValue("number exponent is outside the two-byte key range")
    }
    let biased = UInt16(exponent + 32_768)
    var body: [UInt8] = [UInt8(biased >> 8), UInt8(biased & 0xff)]
    body.append(contentsOf: magnitude.digits.map { $0 - 47 })
    body.append(0)

    if number < .zero {
      output.append(16)
      output.append(contentsOf: body.map { 255 - $0 })
    } else {
      output.append(18)
      output.append(contentsOf: body)
    }
  }

  /// Escapes zero as `0, 255`; `0, 1` ends the component.
  private static func appendEscaped<S: Sequence>(_ bytes: S, to output: inout [UInt8]) where S.Element == UInt8 {
    for byte in bytes {
      if byte == 0 {
        output.append(contentsOf: [0, 255])
      } else {
        output.append(byte)
      }
    }
    output.append(contentsOf: [0, 1])
  }
}
