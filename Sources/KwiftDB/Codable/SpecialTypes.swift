import Foundation

/// Types that map to a `Value` directly instead of through their own
/// `Encodable` / `Decodable` implementation.
///
/// The check runs before the generic path at every depth, so a `Date` inside
/// an array inside a struct is still stored as a number of milliseconds. The
/// mapping:
///
/// | Swift type | `Value` |
/// | --- | --- |
/// | `Value` | itself, unchanged |
/// | `Number` | `.number` |
/// | `Document` | `.document` |
/// | `Data` | `.bytes` |
/// | `Date` | `.number` of integer milliseconds since 1970 |
/// | `UUID` | `.text`, lowercased |
/// | `URL` | `.text(absoluteString)` |
/// | `Int128`, `UInt128` | `.number`, exact |
enum SpecialTypes {
  /// Returns the direct `Value` for `value`, or `nil` when it has no special form.
  static func encode<T: Encodable>(_ value: T, codingPath: [any CodingKey]) throws -> Value? {
    switch value {
    case let value as Value: return value
    case let number as Number: return .number(number)
    case let document as Document: return .document(document)
    case let data as Data: return .bytes([UInt8](data))
    case let date as Date: return try encodeDate(date, codingPath: codingPath)
    case let uuid as UUID: return .text(uuid.uuidString.lowercased())
    case let url as URL: return .text(url.absoluteString)
    default: break
    }
    if #available(macOS 15, iOS 18, *) {
      if let wide = value as? Int128 { return try wide.encodedScalar(codingPath: codingPath) }
      if let wide = value as? UInt128 { return try wide.encodedScalar(codingPath: codingPath) }
    }
    return nil
  }

  /// A date becomes the nearest whole number of milliseconds since 1970.
  private static func encodeDate(_ date: Date, codingPath: [any CodingKey]) throws -> Value {
    let milliseconds = (date.timeIntervalSince1970 * 1000).rounded()
    guard milliseconds.isFinite, let exact = Int64(exactly: milliseconds) else {
      throw CodingErrors.invalidValue(
        date, "Date is outside the range representable as milliseconds since 1970.",
        codingPath: codingPath)
    }
    return try exact.encodedScalar(codingPath: codingPath)
  }

  /// Returns the directly decoded `T` for `value`, or `nil` when `T` has no special form.
  static func decode<T: Decodable>(
    _ type: T.Type, from value: Value, codingPath: [any CodingKey]
  ) throws -> T? {
    if type == Value.self { return (value as! T) }
    if type == Number.self {
      guard case .number(let number) = value else {
        throw CodingErrors.typeMismatch(type, found: value, codingPath: codingPath)
      }
      return (number as! T)
    }
    if type == Document.self {
      guard case .document(let document) = value else {
        throw CodingErrors.typeMismatch(type, found: value, codingPath: codingPath)
      }
      return (document as! T)
    }
    if type == Data.self {
      guard case .bytes(let bytes) = value else {
        throw CodingErrors.typeMismatch(type, found: value, codingPath: codingPath)
      }
      return (Data(bytes) as! T)
    }
    if type == Date.self {
      guard case .number(let number) = value else {
        throw CodingErrors.typeMismatch(type, found: value, codingPath: codingPath)
      }
      guard let milliseconds = NumberParts(number).truncatedInt64 else {
        throw CodingErrors.dataCorrupted(
          "Number \(number) is out of range for a date.", codingPath: codingPath)
      }
      return (Date(timeIntervalSince1970: Double(milliseconds) / 1000) as! T)
    }
    if type == UUID.self {
      guard case .text(let text) = value else {
        throw CodingErrors.typeMismatch(type, found: value, codingPath: codingPath)
      }
      guard let uuid = UUID(uuidString: text) else {
        throw CodingErrors.dataCorrupted("Invalid UUID string \"\(text)\".", codingPath: codingPath)
      }
      return (uuid as! T)
    }
    if type == URL.self {
      guard case .text(let text) = value else {
        throw CodingErrors.typeMismatch(type, found: value, codingPath: codingPath)
      }
      guard let url = URL(string: text) else {
        throw CodingErrors.dataCorrupted("Invalid URL string \"\(text)\".", codingPath: codingPath)
      }
      return (url as! T)
    }
    if #available(macOS 15, iOS 18, *) {
      if type == Int128.self {
        return (try Int128.decodedScalar(from: value, codingPath: codingPath) as! T)
      }
      if type == UInt128.self {
        return (try UInt128.decodedScalar(from: value, codingPath: codingPath) as! T)
      }
    }
    return nil
  }
}
