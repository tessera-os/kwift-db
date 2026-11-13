import Foundation

/// Decodes `Decodable` values from KwiftDB `Value`s; the inverse of `ValueEncoder`.
///
/// ## Mapping
///
/// - A keyed container reads a `.document`; an unkeyed container reads a
///   `.list`; a single value reads `.text`, `.bool` or `.number` as the
///   requested Swift type. Asking for the wrong case throws
///   `DecodingError.typeMismatch` whose message names the case found.
/// - An absent field and a `.null` field both decode to `nil` for optionals and
///   `decodeIfPresent`; `decodeNil(forKey:)` is true for both. Asking for a
///   non-optional value of an absent field throws `keyNotFound`.
/// - Integer targets require an exact, in-range number: a fraction or an
///   overflow throws `DecodingError.dataCorrupted` with the coding path.
///   `Float` and `Double` accept any number that is finite in the target type.
///
/// ## Special types
///
/// `Value`, `Number`, `Document`, `Data` (from `.bytes`), `Date` (from any
/// `.number`, in milliseconds since 1970, rounded toward zero), `UUID` and
/// `URL` (from `.text`) are decoded directly at any depth.
///
/// `ValueDecoder` is a value type; configure `userInfo` before decoding.
public struct ValueDecoder: Sendable {
  /// Contextual information made available to `Decodable.init(from:)` through
  /// `Decoder.userInfo`.
  public var userInfo: [CodingUserInfoKey: any Sendable] = [:]

  /// Creates a decoder with empty `userInfo`.
  public init() {}

  /// Decodes a value of `type` from `value`.
  ///
  /// - Parameters:
  ///   - type: The type to decode.
  ///   - value: The stored value to read.
  /// - Throws: A standard `DecodingError` whose `codingPath` points at the
  ///   offending value.
  public func decode<T: Decodable>(_ type: T.Type, from value: Value) throws -> T {
    try decodeValue(
      type, from: value, codingPath: [], userInfo: userInfo.mapValues { $0 as Any })
  }
}

extension Document {
  /// Decodes a `T` from this document with `ValueDecoder`.
  ///
  /// - Parameter type: The type to decode.
  /// - Throws: A `DecodingError` describing the first field that did not fit.
  public func decode<T: Decodable>(_ type: T.Type) throws -> T {
    try ValueDecoder().decode(type, from: .document(self))
  }
}
