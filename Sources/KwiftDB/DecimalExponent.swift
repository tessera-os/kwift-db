import Foundation

/// A signed, arbitrary-precision base-ten integer used for decimal exponents.
///
/// Exponents are kept as decimal digits rather than a machine integer so a
/// `Number` can preserve values whose scale is larger than the platform's
/// integer range. The type is internal because callers work with `Number`.
internal struct DecimalExponent: Sendable, Hashable, Comparable, CustomStringConvertible {
  private(set) var sign: Int8
  private(set) var digits: [UInt8]

  static let zero = DecimalExponent(sign: 0, digits: [48])

  init(_ text: Substring) throws {
    guard !text.isEmpty else { throw KwiftDBError.invalidNumber("empty exponent") }
    var source = Array(text.utf8)
    var parsedSign: Int8 = 1
    if source.first == 43 {
      source.removeFirst()
    } else if source.first == 45 {
      parsedSign = -1
      source.removeFirst()
    }
    guard !source.isEmpty, source.allSatisfy({ 48...57 ~= $0 }) else {
      throw KwiftDBError.invalidNumber("exponent must contain decimal digits")
    }
    let firstNonzero = source.firstIndex(where: { $0 != 48 }) ?? source.endIndex
    let normalized = Array(source[firstNonzero...])
    if normalized.isEmpty {
      self = .zero
    } else {
      sign = parsedSign
      digits = normalized
    }
  }

  private init(sign: Int8, digits: [UInt8]) {
    let firstNonzero = digits.firstIndex(where: { $0 != 48 }) ?? digits.endIndex
    let normalized = Array(digits[firstNonzero...])
    if normalized.isEmpty {
      self.sign = 0
      self.digits = [48]
    } else {
      self.sign = sign
      self.digits = normalized
    }
  }

  func adding(_ value: Int) -> DecimalExponent {
    guard value != 0 else { return self }
    let magnitude = Array(String(value.magnitude).utf8)
    let other = DecimalExponent(sign: value < 0 ? -1 : 1, digits: magnitude)
    if sign == 0 { return other }
    if other.sign == 0 { return self }
    if sign == other.sign {
      return DecimalExponent(sign: sign, digits: Self.addMagnitudes(digits, other.digits))
    }
    switch Self.compareMagnitudes(digits, other.digits) {
    case .orderedSame:
      return .zero
    case .orderedDescending:
      return DecimalExponent(sign: sign, digits: Self.subtractMagnitudes(digits, other.digits))
    case .orderedAscending:
      return DecimalExponent(sign: other.sign, digits: Self.subtractMagnitudes(other.digits, digits))
    }
  }

  var intValue: Int? { Int(description) }

  var description: String {
    let magnitude = String(decoding: digits, as: UTF8.self)
    return sign < 0 ? "-" + magnitude : magnitude
  }

  static func < (lhs: DecimalExponent, rhs: DecimalExponent) -> Bool {
    if lhs.sign != rhs.sign { return lhs.sign < rhs.sign }
    guard lhs.sign != 0 else { return false }
    let order = compareMagnitudes(lhs.digits, rhs.digits)
    return lhs.sign > 0 ? order == .orderedAscending : order == .orderedDescending
  }

  private static func compareMagnitudes(_ lhs: [UInt8], _ rhs: [UInt8]) -> ComparisonResult {
    if lhs.count != rhs.count { return lhs.count < rhs.count ? .orderedAscending : .orderedDescending }
    for (left, right) in zip(lhs, rhs) where left != right {
      return left < right ? .orderedAscending : .orderedDescending
    }
    return .orderedSame
  }

  private static func addMagnitudes(_ lhs: [UInt8], _ rhs: [UInt8]) -> [UInt8] {
    var result: [UInt8] = []
    result.reserveCapacity(max(lhs.count, rhs.count) + 1)
    var left = lhs.count - 1
    var right = rhs.count - 1
    var carry: UInt8 = 0
    while left >= 0 || right >= 0 || carry != 0 {
      let a = left >= 0 ? lhs[left] - 48 : 0
      let b = right >= 0 ? rhs[right] - 48 : 0
      let sum = a + b + carry
      result.append(48 + sum % 10)
      carry = sum / 10
      left -= 1
      right -= 1
    }
    return result.reversed()
  }

  /// Subtracts `rhs` from `lhs`; callers guarantee `lhs >= rhs`.
  private static func subtractMagnitudes(_ lhs: [UInt8], _ rhs: [UInt8]) -> [UInt8] {
    var result: [UInt8] = []
    result.reserveCapacity(lhs.count)
    var left = lhs.count - 1
    var right = rhs.count - 1
    var borrow: Int16 = 0
    while left >= 0 {
      var digit = Int16(lhs[left] - 48) - borrow
      let subtract = right >= 0 ? Int16(rhs[right] - 48) : 0
      if digit < subtract {
        digit += 10
        borrow = 1
      } else {
        borrow = 0
      }
      result.append(UInt8(48 + digit - subtract))
      left -= 1
      right -= 1
    }
    while result.count > 1 && result.last == 48 { result.removeLast() }
    return result.reversed()
  }
}
