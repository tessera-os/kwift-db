import Foundation

/// An exact finite decimal with arbitrary significant digits and exponent.
///
/// The original decimal spelling is not retained. Equal decimal values have a
/// single normalized representation, and no binary floating-point conversion
/// occurs during parsing, comparison, hashing, or encoding. This type backs
/// `Value.number` and the order-preserving numeric key format; using a custom
/// representation keeps persisted values independent of `Double` and of the
/// precision limit of Foundation's `Decimal`.
public struct Number: Sendable, Hashable, Comparable, CustomStringConvertible {
  private let sign: Int8
  private let digits: [UInt8]
  private let scale: DecimalExponent

  /// Parses a decimal string without passing through a floating-point value.
  ///
  /// Accepted forms include an optional sign, a decimal point, and an optional
  /// base-ten exponent, such as `-12.50`, `.125`, or `3e400`. Leading and
  /// trailing zeroes are normalized, while the represented decimal value is
  /// preserved exactly. `NaN`, infinities, whitespace, and malformed exponents
  /// are rejected.
  ///
  /// - Parameter text: A finite base-ten decimal representation.
  /// - Throws: `KwiftDBError.invalidNumber` when the text is not a decimal.
  public init(_ text: String) throws {
    guard !text.isEmpty else { throw KwiftDBError.invalidNumber("empty text") }

    let exponentSplit = text.split(omittingEmptySubsequences: false) { $0 == "e" || $0 == "E" }
    guard exponentSplit.count <= 2 else {
      throw KwiftDBError.invalidNumber("more than one exponent marker")
    }

    let explicitExponent: DecimalExponent
    if exponentSplit.count == 2 {
      explicitExponent = try DecimalExponent(exponentSplit[1])
    } else {
      explicitExponent = .zero
    }

    var mantissa = String(exponentSplit[0])
    var parsedSign: Int8 = 1
    if mantissa.first == "+" {
      mantissa.removeFirst()
    } else if mantissa.first == "-" {
      parsedSign = -1
      mantissa.removeFirst()
    }
    guard !mantissa.isEmpty else { throw KwiftDBError.invalidNumber("missing digits") }

    let decimalParts = mantissa.split(separator: ".", omittingEmptySubsequences: false)
    guard decimalParts.count <= 2 else {
      throw KwiftDBError.invalidNumber("more than one decimal point")
    }
    let whole = String(decimalParts[0])
    let fraction = decimalParts.count == 2 ? String(decimalParts[1]) : ""
    guard (!whole.isEmpty || !fraction.isEmpty),
          whole.utf8.allSatisfy({ 48...57 ~= $0 }),
          fraction.utf8.allSatisfy({ 48...57 ~= $0 })
    else {
      throw KwiftDBError.invalidNumber("expected decimal digits")
    }

    let sourceDigits = Array((whole + fraction).utf8)
    guard let firstNonzero = sourceDigits.firstIndex(where: { $0 != 48 }) else {
      sign = 0
      digits = [48]
      scale = .zero
      return
    }

    let significant = Array(sourceDigits[firstNonzero...])
    let trailingZeros = significant.reversed().prefix(while: { $0 == 48 }).count
    let normalizedDigits = Array(significant.dropLast(trailingZeros))
    sign = parsedSign
    digits = normalizedDigits
    scale = explicitExponent.adding(-fraction.utf8.count).adding(trailingZeros)
  }

  /// Creates an exact number from a signed 64-bit integer.
  ///
  /// This convenience initializer only formats the integer as decimal text;
  /// it does not convert the value to `Double`.
  ///
  /// - Parameter value: The integer to represent.
  public init(_ value: Int64) {
    let text = String(value)
    if let parsed = try? Number(text) {
      self = parsed
    } else {
      self = Number.zero
    }
  }

  /// The numeric value zero. Positive and negative textual zero parse to this
  /// same value, matching numeric key ordering.
  public static let zero = Number(sign: 0, digits: [48], scale: .zero)

  /// Whether this value is numerically zero, regardless of the input spelling.
  public var isZero: Bool { sign == 0 }

  /// A canonical decimal spelling suitable for the database value codec.
  ///
  /// Ordinary values use decimal notation. Values with very large exponents
  /// use scientific notation so reading one value does not allocate a string
  /// proportional to its exponent. Either spelling reparses to the same exact
  /// value.
  public var description: String {
    guard sign != 0 else { return "0" }

    let exponent = scale.adding(digits.count - 1)
    let prefix = sign < 0 ? "-" : ""
    guard let expandedScale = scale.intValue, (-256...256).contains(expandedScale) else {
      let head = String(decoding: digits.prefix(1), as: UTF8.self)
      let tail = digits.count > 1 ? "." + String(decoding: digits.dropFirst(), as: UTF8.self) : ""
      return "\(prefix)\(head)\(tail)e\(exponent)"
    }

    let raw = String(decoding: digits, as: UTF8.self)
    if expandedScale >= 0 {
      if digits.count + expandedScale <= 256 {
        return prefix + raw + String(repeating: "0", count: expandedScale)
      }
      let head = String(decoding: digits.prefix(1), as: UTF8.self)
      let tail = digits.count > 1 ? "." + String(decoding: digits.dropFirst(), as: UTF8.self) : ""
      return "\(prefix)\(head)\(tail)e\(exponent)"
    }
    let decimalPosition = digits.count + expandedScale
    if decimalPosition > 0 {
      let split = raw.index(raw.startIndex, offsetBy: decimalPosition)
      return prefix + raw[..<split] + "." + raw[split...]
    }
    if -decimalPosition + digits.count + 2 <= 256 {
      return prefix + "0." + String(repeating: "0", count: -decimalPosition) + raw
    }
    let head = String(decoding: digits.prefix(1), as: UTF8.self)
    let tail = digits.count > 1 ? "." + String(decoding: digits.dropFirst(), as: UTF8.self) : ""
    return "\(prefix)\(head)\(tail)e\(exponent)"
  }

  /// The exponent and significant digits used by the order-preserving key codec.
  internal var keyMagnitude: (exponent: Int?, digits: [UInt8]) {
    (scale.adding(digits.count - 1).intValue, digits)
  }

  /// A decimal spelling for a non-negative integer, without scientific form.
  ///
  /// Storage counters have their own arbitrary-width integer implementation;
  /// this projection lets them validate exact `Number` values without parsing
  /// the user-facing compact description, which may use an exponent.
  internal var unsignedIntegerText: String? {
    guard sign >= 0 else { return nil }
    if sign == 0 { return "0" }
    guard let integerScale = scale.intValue, integerScale >= 0 else { return nil }
    return String(decoding: digits, as: UTF8.self) + String(repeating: "0", count: integerScale)
  }

  public static func == (lhs: Number, rhs: Number) -> Bool {
    lhs.sign == rhs.sign && lhs.scale == rhs.scale && lhs.digits == rhs.digits
  }

  public func hash(into hasher: inout Hasher) {
    hasher.combine(sign)
    hasher.combine(scale)
    hasher.combine(digits)
  }

  public static func < (lhs: Number, rhs: Number) -> Bool {
    if lhs.sign != rhs.sign { return lhs.sign < rhs.sign }
    guard lhs.sign != 0 else { return false }

    let leftExponent = lhs.scale.adding(lhs.digits.count - 1)
    let rightExponent = rhs.scale.adding(rhs.digits.count - 1)
    if leftExponent != rightExponent {
      return lhs.sign > 0 ? leftExponent < rightExponent : leftExponent > rightExponent
    }

    let width = max(lhs.digits.count, rhs.digits.count)
    for index in 0..<width {
      let left = index < lhs.digits.count ? lhs.digits[index] : 48
      let right = index < rhs.digits.count ? rhs.digits[index] : 48
      if left != right { return lhs.sign > 0 ? left < right : left > right }
    }
    return false
  }

  private init(sign: Int8, digits: [UInt8], scale: DecimalExponent) {
    self.sign = sign
    self.digits = digits
    self.scale = scale
  }
}
