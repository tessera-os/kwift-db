import Foundation

/// The integer part and fraction flag of a `Number`, computed from its exact
/// decimal spelling without going through `Double`.
///
/// `Number` does not expose its digits, so this reads its canonical
/// `description` (decimal or scientific) and works out where the decimal
/// point falls. It is what makes integer decoding exact and what lets `Date`
/// decoding round toward zero.
struct NumberParts {
  /// Whether the number is below zero.
  var isNegative = false
  /// The integer digits with leading zeros removed. Empty means the integer part is 0.
  var integerDigits: [UInt8] = []
  /// Whether the number has a nonzero fractional part.
  var hasFraction = false
  /// Whether the integer part is too large to track (more than 30 digits).
  var integerOverflow = false

  /// Analyses `number`.
  init(_ number: Number) {
    var text = number.description
    if text.hasPrefix("-") {
      isNegative = true
      text.removeFirst()
    }
    let split = text.split(separator: "e", omittingEmptySubsequences: false)
    var exponent = 0
    if split.count == 2 {
      let raw = split[1].replacingOccurrences(of: "+", with: "")
      exponent = Int(raw) ?? (raw.hasPrefix("-") ? -1_000_000 : 1_000_000)
      exponent = Swift.max(-1_000_000, Swift.min(1_000_000, exponent))
    }
    let mantissa = split[0].split(separator: ".", omittingEmptySubsequences: false)
    let whole = Array(mantissa[0].utf8)
    let fraction = mantissa.count > 1 ? Array(mantissa[1].utf8) : []
    let digits = whole + fraction
    guard digits.contains(where: { $0 != 48 }) else {
      isNegative = false
      return
    }
    let point = whole.count + exponent
    var integer: [UInt8]
    if point >= digits.count {
      if point > 30 {
        integerOverflow = true
        return
      }
      integer = digits + [UInt8](repeating: 48, count: point - digits.count)
    } else if point <= 0 {
      hasFraction = true
      return
    } else {
      integer = Array(digits[..<point])
      hasFraction = digits[point...].contains(where: { $0 != 48 })
    }
    if let first = integer.firstIndex(where: { $0 != 48 }) {
      integer = Array(integer[first...])
    } else {
      integer = []
    }
    integerDigits = integer
  }

  /// The magnitude of the integer part, or `nil` if it exceeds `UInt64`.
  var magnitude: UInt64? {
    guard !integerOverflow, integerDigits.count <= 20 else { return nil }
    guard !integerDigits.isEmpty else { return 0 }
    return UInt64(String(decoding: integerDigits, as: UTF8.self))
  }

  /// The integer part (fraction dropped, toward zero) as `Int64`, or `nil` on overflow.
  var truncatedInt64: Int64? {
    guard let magnitude else { return nil }
    if isNegative {
      if magnitude == UInt64(Int64.max) + 1 { return Int64.min }
      guard magnitude <= UInt64(Int64.max) else { return nil }
      return -Int64(magnitude)
    }
    return Int64(exactly: magnitude)
  }
}

/// A Swift scalar that has a direct `Value` form.
///
/// Containers use this to share one implementation across every `encode(_:)`
/// overload of the Codable container protocols.
protocol ValueScalarEncodable {
  /// The `Value` for this scalar.
  ///
  /// - Throws: `EncodingError.invalidValue` for NaN or infinity.
  func encodedScalar(codingPath: [any CodingKey]) throws -> Value
}

/// A Swift scalar that can be read back from a `Value`.
protocol ValueScalarDecodable {
  /// Reads the scalar from `value`.
  ///
  /// - Throws: `DecodingError.typeMismatch` for the wrong `Value` case and
  ///   `DecodingError.dataCorrupted` for a number that is inexact or out of range.
  static func decodedScalar(from value: Value, codingPath: [any CodingKey]) throws -> Self
}

extension ValueScalarEncodable where Self: FixedWidthInteger {
  /// Integers become exact `Number`s via their decimal text.
  func encodedScalar(codingPath: [any CodingKey]) throws -> Value {
    do { return .number(try Number(String(self))) } catch {
      throw CodingErrors.invalidValue(self, "\(error)", codingPath: codingPath)
    }
  }
}

extension ValueScalarDecodable where Self: FixedWidthInteger {
  /// Integers must be exact and within range.
  static func decodedScalar(from value: Value, codingPath: [any CodingKey]) throws -> Self {
    guard case .number(let number) = value else {
      throw CodingErrors.typeMismatch(Self.self, found: value, codingPath: codingPath)
    }
    let parts = NumberParts(number)
    guard !parts.hasFraction else {
      throw CodingErrors.dataCorrupted(
        "Number \(number) has a fractional part and cannot be decoded as \(Self.self).",
        codingPath: codingPath)
    }
    let result: Self?
    if let magnitude = parts.magnitude {
      if parts.isNegative {
        if magnitude == UInt64(Int64.max) + 1 {
          result = Self(exactly: Int64.min)
        } else if magnitude <= UInt64(Int64.max) {
          result = Self(exactly: -Int64(magnitude))
        } else {
          result = nil
        }
      } else {
        result = Self(exactly: magnitude)
      }
    } else if #available(macOS 15, iOS 18, *), Self.self == UInt128.self || Self.self == Int128.self {
      result = wideInteger(parts)
    } else {
      result = nil
    }
    guard let result else {
      throw CodingErrors.dataCorrupted(
        "Number \(number) does not fit in \(Self.self).", codingPath: codingPath)
    }
    return result
  }

  /// Parses an integer wider than 64 bits (only reachable for 128-bit types).
  private static func wideInteger(_ parts: NumberParts) -> Self? {
    guard !parts.integerOverflow else { return nil }
    let text = (parts.isNegative ? "-" : "") + String(decoding: parts.integerDigits, as: UTF8.self)
    return Self(text, radix: 10)
  }
}

extension Int: ValueScalarEncodable, ValueScalarDecodable {}
extension Int8: ValueScalarEncodable, ValueScalarDecodable {}
extension Int16: ValueScalarEncodable, ValueScalarDecodable {}
extension Int32: ValueScalarEncodable, ValueScalarDecodable {}
extension Int64: ValueScalarEncodable, ValueScalarDecodable {}
extension UInt: ValueScalarEncodable, ValueScalarDecodable {}
extension UInt8: ValueScalarEncodable, ValueScalarDecodable {}
extension UInt16: ValueScalarEncodable, ValueScalarDecodable {}
extension UInt32: ValueScalarEncodable, ValueScalarDecodable {}
extension UInt64: ValueScalarEncodable, ValueScalarDecodable {}

extension Bool: ValueScalarEncodable, ValueScalarDecodable {
  func encodedScalar(codingPath: [any CodingKey]) throws -> Value { .bool(self) }
  static func decodedScalar(from value: Value, codingPath: [any CodingKey]) throws -> Bool {
    guard case .bool(let flag) = value else {
      throw CodingErrors.typeMismatch(Bool.self, found: value, codingPath: codingPath)
    }
    return flag
  }
}

extension String: ValueScalarEncodable, ValueScalarDecodable {
  func encodedScalar(codingPath: [any CodingKey]) throws -> Value { .text(self) }
  static func decodedScalar(from value: Value, codingPath: [any CodingKey]) throws -> String {
    guard case .text(let text) = value else {
      throw CodingErrors.typeMismatch(String.self, found: value, codingPath: codingPath)
    }
    return text
  }
}

/// Builds the `Number` for a finite floating-point value using its shortest
/// round-tripping decimal text (`description`), never a binary expansion.
private func floatingNumber(
  text: String, finite: Bool, original: Any, codingPath: [any CodingKey]
) throws -> Value {
  guard finite else {
    throw CodingErrors.invalidValue(
      original, "Cannot encode \(text) as a number; NaN and infinity are not representable.",
      codingPath: codingPath)
  }
  do { return .number(try Number(text.replacingOccurrences(of: "+", with: ""))) } catch {
    throw CodingErrors.invalidValue(original, "\(error)", codingPath: codingPath)
  }
}

/// Reads a finite floating-point value from a number's decimal text.
private func floatingText(
  _ type: Any.Type, from value: Value, codingPath: [any CodingKey]
) throws -> String {
  guard case .number(let number) = value else {
    throw CodingErrors.typeMismatch(type, found: value, codingPath: codingPath)
  }
  return number.description
}

extension Double: ValueScalarEncodable, ValueScalarDecodable {
  func encodedScalar(codingPath: [any CodingKey]) throws -> Value {
    try floatingNumber(text: description, finite: isFinite, original: self, codingPath: codingPath)
  }
  static func decodedScalar(from value: Value, codingPath: [any CodingKey]) throws -> Double {
    let text = try floatingText(Double.self, from: value, codingPath: codingPath)
    guard let result = Double(text), result.isFinite else {
      throw CodingErrors.dataCorrupted(
        "Number \(text) does not fit in Double.", codingPath: codingPath)
    }
    return result
  }
}

extension Float: ValueScalarEncodable, ValueScalarDecodable {
  func encodedScalar(codingPath: [any CodingKey]) throws -> Value {
    try floatingNumber(text: description, finite: isFinite, original: self, codingPath: codingPath)
  }
  static func decodedScalar(from value: Value, codingPath: [any CodingKey]) throws -> Float {
    let text = try floatingText(Float.self, from: value, codingPath: codingPath)
    guard let result = Float(text), result.isFinite else {
      throw CodingErrors.dataCorrupted(
        "Number \(text) does not fit in Float.", codingPath: codingPath)
    }
    return result
  }
}

@available(macOS 15, iOS 18, *)
extension Int128: ValueScalarEncodable, ValueScalarDecodable {}
@available(macOS 15, iOS 18, *)
extension UInt128: ValueScalarEncodable, ValueScalarDecodable {}
