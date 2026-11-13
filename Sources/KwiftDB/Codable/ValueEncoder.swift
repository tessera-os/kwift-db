import Foundation

/// Encodes `Encodable` values into KwiftDB `Value`s.
///
/// ## Mapping
///
/// - A keyed container becomes `.document`, fields in the order they were
///   encoded. `encodeIfPresent(nil)` and `nil` optional properties of
///   synthesized `Encodable` types leave the field out; encoding an explicit
///   null writes `.null`. Encoding the same key twice replaces the first value
///   in place.
/// - An unkeyed container becomes `.list`.
/// - A single value: `String` becomes `.text`, `Bool` becomes `.bool`, every
///   integer width and `Float` / `Double` become `.number`. Integers are
///   exact; floating-point values use the shortest decimal text that
///   round-trips. NaN and infinity throw `EncodingError.invalidValue`.
/// - A type that encodes nothing becomes an empty document.
///
/// ## Special types
///
/// These are checked before generic encoding, at any depth:
/// `Value` (unchanged), `Number` (`.number`), `Document` (`.document`),
/// `Data` (`.bytes`), `Date` (`.number` of whole milliseconds since 1970),
/// `UUID` (lowercased `.text`) and `URL` (`.text(absoluteString)`).
///
/// ## Dictionaries
///
/// Swift's own `Dictionary` conformance decides the shape: a `String`-keyed
/// (or `Int`-keyed, with the decimal text as field name) dictionary is encoded
/// through a keyed container, so it becomes a `.document`. Any other key type
/// is encoded by Swift as an unkeyed container of alternating keys and values,
/// so it becomes a `.list` `[key, value, key, value, ...]`. This coder does not
/// add anything on top of that.
///
/// `ValueEncoder` is a value type; configure `userInfo` before encoding.
public struct ValueEncoder: Sendable {
  /// Contextual information made available to `Encodable.encode(to:)` through
  /// `Encoder.userInfo`.
  public var userInfo: [CodingUserInfoKey: any Sendable] = [:]

  /// Creates an encoder with empty `userInfo`.
  public init() {}

  /// Encodes `value` into a `Value`.
  ///
  /// - Parameter value: Any `Encodable` value.
  /// - Returns: The encoded value. Failures are standard `EncodingError`s whose
  ///   `codingPath` points at the offending value.
  public func encode<T: Encodable>(_ value: T) throws -> Value {
    let root = EncodingNode()
    let info = userInfo.mapValues { $0 as Any }
    try encodeValue(value, into: root, codingPath: [], userInfo: info)
    return try root.resolve()
  }
}

extension Document {
  /// Creates a document by encoding `value` with `ValueEncoder`.
  ///
  /// - Parameter value: A value that encodes as a keyed container (a struct,
  ///   a class, a `[String: T]`, or a `Document`).
  /// - Throws: `EncodingError.invalidValue` if the top-level value does not
  ///   encode as a document, or any error `ValueEncoder` throws.
  public init<T: Encodable>(encoding value: T) throws {
    let encoded = try ValueEncoder().encode(value)
    guard case .document(let document) = encoded else {
      throw CodingErrors.invalidValue(
        value,
        "Top-level value must encode as a document but encoded as \(encoded.caseName).",
        codingPath: [])
    }
    self = document
  }
}
