/// Decodes a `T` from `value`, applying the special-type mapping first.
///
/// This is the single entry point every container and the public
/// `ValueDecoder` use for "decode an arbitrary `Decodable`".
func decodeValue<T: Decodable>(
  _ type: T.Type, from value: Value, codingPath: [any CodingKey],
  userInfo: [CodingUserInfoKey: Any]
) throws -> T {
  if let direct = try SpecialTypes.decode(type, from: value, codingPath: codingPath) {
    return direct
  }
  let decoder = InternalValueDecoder(value: value, codingPath: codingPath, userInfo: userInfo)
  return try T(from: decoder)
}

/// The `Decoder` handed to `Decodable.init(from:)`; it reads one `Value`.
struct InternalValueDecoder: Decoder {
  /// The value this decoder reads.
  let value: Value
  let codingPath: [any CodingKey]
  let userInfo: [CodingUserInfoKey: Any]

  func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
    guard case .document(let document) = value else {
      throw CodingErrors.typeMismatch(
        KeyedDecodingContainer<Key>.self, found: value, codingPath: codingPath)
    }
    return KeyedDecodingContainer(
      ValueKeyedDecodingContainer<Key>(
        document: document, codingPath: codingPath, userInfo: userInfo))
  }

  func unkeyedContainer() throws -> any UnkeyedDecodingContainer {
    guard case .list(let elements) = value else {
      throw CodingErrors.typeMismatch(
        (any UnkeyedDecodingContainer).self, found: value, codingPath: codingPath)
    }
    return ValueUnkeyedDecodingContainer(
      elements: elements, codingPath: codingPath, userInfo: userInfo)
  }

  func singleValueContainer() throws -> any SingleValueDecodingContainer {
    ValueSingleValueDecodingContainer(value: value, codingPath: codingPath, userInfo: userInfo)
  }
}

/// A single value container over one `Value`.
///
/// `decodeNil()` is true exactly for `.null`.
struct ValueSingleValueDecodingContainer: SingleValueDecodingContainer {
  let value: Value
  let codingPath: [any CodingKey]
  let userInfo: [CodingUserInfoKey: Any]

  func decodeNil() -> Bool { value == .null }

  func decode(_ type: Bool.Type) throws -> Bool { try scalar(type) }
  func decode(_ type: String.Type) throws -> String { try scalar(type) }
  func decode(_ type: Double.Type) throws -> Double { try scalar(type) }
  func decode(_ type: Float.Type) throws -> Float { try scalar(type) }
  func decode(_ type: Int.Type) throws -> Int { try scalar(type) }
  func decode(_ type: Int8.Type) throws -> Int8 { try scalar(type) }
  func decode(_ type: Int16.Type) throws -> Int16 { try scalar(type) }
  func decode(_ type: Int32.Type) throws -> Int32 { try scalar(type) }
  func decode(_ type: Int64.Type) throws -> Int64 { try scalar(type) }
  func decode(_ type: UInt.Type) throws -> UInt { try scalar(type) }
  func decode(_ type: UInt8.Type) throws -> UInt8 { try scalar(type) }
  func decode(_ type: UInt16.Type) throws -> UInt16 { try scalar(type) }
  func decode(_ type: UInt32.Type) throws -> UInt32 { try scalar(type) }
  func decode(_ type: UInt64.Type) throws -> UInt64 { try scalar(type) }

  func decode<T: Decodable>(_ type: T.Type) throws -> T {
    try decodeValue(type, from: value, codingPath: codingPath, userInfo: userInfo)
  }

  private func scalar<S: ValueScalarDecodable>(_ type: S.Type) throws -> S {
    try S.decodedScalar(from: value, codingPath: codingPath)
  }
}
