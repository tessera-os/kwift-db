/// An arbitrary-width unsigned integer used for durable sequence and file ids.
///
/// The storage specification encodes these values with an unbounded base-128
/// varint. Keeping their decimal digits here avoids imposing a `UInt64` limit
/// on a database that may be reopened after a very long lifetime.
internal struct DatabaseUInt: Sendable, Hashable, Comparable, CustomStringConvertible {
  private(set) var digits: [UInt8]

  /// The value zero.
  static let zero = DatabaseUInt(normalizedDigits: [48])
  /// The value one.
  static let one = DatabaseUInt(normalizedDigits: [49])

  /// Parses a non-negative, base-ten integer without leading sign characters.
  init(_ text: String) throws {
    let bytes = Array(text.utf8)
    guard !bytes.isEmpty, bytes.allSatisfy({ 48...57 ~= $0 }) else {
      throw KwiftDBError.invalidEncoding("expected an unsigned decimal integer")
    }
    self.init(normalizedDigits: bytes)
  }

  /// Creates an arbitrary-width value from a machine-sized non-negative value.
  init(_ value: UInt64) {
    self.init(normalizedDigits: Array(String(value).utf8))
  }

  private init(normalizedDigits: [UInt8]) {
    let firstNonzero = normalizedDigits.firstIndex(where: { $0 != 48 }) ?? normalizedDigits.endIndex
    let significant = Array(normalizedDigits[firstNonzero...])
    digits = significant.isEmpty ? [48] : significant
  }

  /// Whether the integer is zero.
  var isZero: Bool { digits == [48] }

  /// The shortest base-ten representation.
  var description: String { String(decoding: digits, as: UTF8.self) }

  /// Converts to `Int` when this value fits the current process.
  var intValue: Int? { Int(description) }

  /// Converts to `UInt64` when this value fits the machine word.
  var uint64Value: UInt64? { UInt64(description) }

  /// Returns this number plus one.
  func incremented() -> DatabaseUInt {
    var result = digits
    var index = result.count - 1
    while true {
      if result[index] < 57 {
        result[index] += 1
        break
      }
      result[index] = 48
      if index == 0 {
        result.insert(49, at: 0)
        break
      }
      index -= 1
    }
    return DatabaseUInt(normalizedDigits: result)
  }

  /// Adds a machine-sized count without narrowing the stored counter.
  func adding(_ value: UInt64) -> DatabaseUInt {
    var result = digits
    var carry = value
    var index = result.count - 1
    while carry > 0 {
      let digit = UInt64(result[index] - 48) + carry % 10
      result[index] = UInt8(48 + digit % 10)
      carry = carry / 10 + digit / 10
      if index == 0 {
        if carry > 0 { result.insert(contentsOf: String(carry).utf8, at: 0) }
        break
      } else {
        index -= 1
      }
    }
    return DatabaseUInt(normalizedDigits: result)
  }

  /// Returns this value minus one; callers must provide a non-zero value.
  func decremented() -> DatabaseUInt {
    precondition(!isZero)
    var result = digits
    var index = result.count - 1
    while result[index] == 48 {
      result[index] = 57
      index -= 1
    }
    result[index] -= 1
    return DatabaseUInt(normalizedDigits: result)
  }

  /// Encodes this value as seven-bit groups, least-significant group first.
  func varintBytes() -> [UInt8] {
    if isZero { return [0] }
    var quotient = digits
    var groups: [UInt8] = []
    while quotient != [48] {
      let division = Self.divide(quotient, by: 128)
      groups.append(division.remainder)
      quotient = division.quotient
    }
    for index in 0..<(groups.count - 1) { groups[index] |= 0x80 }
    return groups
  }

  /// Decodes a varint from `bytes`, advancing `offset` past its final group.
  ///
  /// The decoder accepts any complete representation, including a redundant
  /// high zero group, because the specification does not require canonical
  /// varints. Truncation is reported as malformed database data.
  static func readVarint(from bytes: [UInt8], offset: inout Int) throws -> DatabaseUInt {
    var groups: [UInt8] = []
    while offset < bytes.count {
      let byte = bytes[offset]
      offset += 1
      groups.append(byte & 0x7f)
      if byte & 0x80 == 0 {
        var value = DatabaseUInt.zero
        for group in groups.reversed() {
          value = value.multiplied(by: 128).adding(group)
        }
        return value
      }
    }
    throw KwiftDBError.invalidEncoding("truncated variable-length integer")
  }

  /// Returns the remainder after division by a positive machine-sized value.
  func remainder(dividingBy divisor: UInt64) -> UInt64 {
    precondition(divisor > 0)
    var remainder: UInt64 = 0
    for digit in digits {
      remainder = (remainder * 10 + UInt64(digit - 48)) % divisor
    }
    return remainder
  }

  static func < (lhs: DatabaseUInt, rhs: DatabaseUInt) -> Bool {
    if lhs.digits.count != rhs.digits.count { return lhs.digits.count < rhs.digits.count }
    return lhs.digits.lexicographicallyPrecedes(rhs.digits)
  }

  private func multiplied(by multiplier: UInt8) -> DatabaseUInt {
    var result = digits
    var carry: UInt16 = 0
    for index in result.indices.reversed() {
      let value = UInt16(result[index] - 48) * UInt16(multiplier) + carry
      result[index] = UInt8(48 + value % 10)
      carry = value / 10
    }
    while carry > 0 {
      result.insert(UInt8(48 + carry % 10), at: 0)
      carry /= 10
    }
    return DatabaseUInt(normalizedDigits: result)
  }

  private func adding(_ value: UInt8) -> DatabaseUInt {
    var result = digits
    var carry = UInt16(value)
    var index = result.count - 1
    while carry > 0 && index >= 0 {
      let digit = UInt16(result[index] - 48) + carry
      result[index] = UInt8(48 + digit % 10)
      carry = digit / 10
      if index == 0 { break }
      index -= 1
    }
    while carry > 0 {
      result.insert(UInt8(48 + carry % 10), at: 0)
      carry /= 10
    }
    return DatabaseUInt(normalizedDigits: result)
  }

  private static func divide(_ digits: [UInt8], by divisor: UInt16) -> (quotient: [UInt8], remainder: UInt8) {
    var quotient: [UInt8] = []
    quotient.reserveCapacity(digits.count)
    var remainder: UInt16 = 0
    for digit in digits {
      let value = remainder * 10 + UInt16(digit - 48)
      quotient.append(UInt8(48 + value / divisor))
      remainder = value % divisor
    }
    let firstNonzero = quotient.firstIndex(where: { $0 != 48 }) ?? quotient.endIndex
    let normalized = Array(quotient[firstNonzero...])
    return (normalized.isEmpty ? [48] : normalized, UInt8(remainder))
  }
}
