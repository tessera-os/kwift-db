extension Value {
  /// The name of this value's case, as used in error messages.
  ///
  /// Type-mismatch errors say what was actually found, for example
  /// "Expected to decode Int but found text instead."
  var caseName: String {
    switch self {
    case .null: "null"
    case .bool: "bool"
    case .number: "number"
    case .text: "text"
    case .bytes: "bytes"
    case .list: "list"
    case .document: "document"
    case .stream: "stream"
    }
  }
}

/// Builders for the standard errors the coder throws.
///
/// All of them are plain `DecodingError` / `EncodingError` values with an
/// accurate `codingPath`, so callers can treat this coder like any other.
enum CodingErrors {
  /// A `Value` of the wrong case was found where `type` was requested.
  static func typeMismatch(
    _ type: Any.Type, found value: Value, codingPath: [any CodingKey]
  ) -> DecodingError {
    .typeMismatch(
      type,
      .init(
        codingPath: codingPath,
        debugDescription: "Expected to decode \(type) but found \(value.caseName) instead."))
  }

  /// The data has the right case but cannot be represented as the target type.
  static func dataCorrupted(_ message: String, codingPath: [any CodingKey]) -> DecodingError {
    .dataCorrupted(.init(codingPath: codingPath, debugDescription: message))
  }

  /// A keyed container has no field for `key`.
  static func keyNotFound(_ key: any CodingKey, codingPath: [any CodingKey]) -> DecodingError {
    .keyNotFound(
      key,
      .init(
        codingPath: codingPath,
        debugDescription: "No value associated with key \(key.stringValue)."))
  }

  /// An unkeyed container ran out of elements while decoding `type`.
  static func listExhausted(_ type: Any.Type, codingPath: [any CodingKey]) -> DecodingError {
    .valueNotFound(
      type,
      .init(
        codingPath: codingPath,
        debugDescription: "Unkeyed container is at end; no element left to decode \(type)."))
  }

  /// `value` cannot be encoded, for example a NaN.
  static func invalidValue(
    _ value: Any, _ message: String, codingPath: [any CodingKey]
  ) -> EncodingError {
    .invalidValue(value, .init(codingPath: codingPath, debugDescription: message))
  }
}
