/// The keyed container: reads fields of a `.document` by name.
///
/// Absence and null are equivalent for optionals: `contains` is true for a
/// `.null` field, `decodeNil(forKey:)` is true for a missing field or a `.null`
/// one, and the default `decodeIfPresent` (which Swift's synthesized
/// `Decodable` uses for optional properties) therefore returns `nil` for both.
struct ValueKeyedDecodingContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
  let codingPath: [any CodingKey]
  let userInfo: [CodingUserInfoKey: Any]
  private let fieldNames: [String]
  private let lookup: [String: Value]

  init(document: Document, codingPath: [any CodingKey], userInfo: [CodingUserInfoKey: Any]) {
    self.codingPath = codingPath
    self.userInfo = userInfo
    self.fieldNames = document.fields.map(\.name)
    self.lookup = Dictionary(
      document.fields.map { ($0.name, $0.value) }, uniquingKeysWith: { first, _ in first })
  }

  /// The keys of the document's fields, in stored order, that map to a `Key`.
  var allKeys: [Key] { fieldNames.compactMap { Key(stringValue: $0) } }

  func contains(_ key: Key) -> Bool { lookup[key.stringValue] != nil }

  func decodeNil(forKey key: Key) throws -> Bool {
    guard let value = lookup[key.stringValue] else { return true }
    return value == .null
  }

  func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool { try scalar(type, key) }
  func decode(_ type: String.Type, forKey key: Key) throws -> String { try scalar(type, key) }
  func decode(_ type: Double.Type, forKey key: Key) throws -> Double { try scalar(type, key) }
  func decode(_ type: Float.Type, forKey key: Key) throws -> Float { try scalar(type, key) }
  func decode(_ type: Int.Type, forKey key: Key) throws -> Int { try scalar(type, key) }
  func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 { try scalar(type, key) }
  func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 { try scalar(type, key) }
  func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 { try scalar(type, key) }
  func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 { try scalar(type, key) }
  func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt { try scalar(type, key) }
  func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 { try scalar(type, key) }
  func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 { try scalar(type, key) }
  func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 { try scalar(type, key) }
  func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 { try scalar(type, key) }

  func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
    try decodeValue(
      type, from: try entry(key), codingPath: codingPath.appending(key), userInfo: userInfo)
  }

  func nestedContainer<NestedKey: CodingKey>(
    keyedBy type: NestedKey.Type, forKey key: Key
  ) throws -> KeyedDecodingContainer<NestedKey> {
    try decoder(for: try entry(key), key: key).container(keyedBy: type)
  }

  func nestedUnkeyedContainer(forKey key: Key) throws -> any UnkeyedDecodingContainer {
    try decoder(for: try entry(key), key: key).unkeyedContainer()
  }

  /// A decoder over the field named `super`; a missing field reads as null.
  func superDecoder() throws -> any Decoder {
    decoder(for: lookup[ValueCodingKey.super.stringValue] ?? .null, key: ValueCodingKey.super)
  }

  /// A decoder over the field named by `key`; a missing field reads as null.
  func superDecoder(forKey key: Key) throws -> any Decoder {
    decoder(for: lookup[key.stringValue] ?? .null, key: key)
  }

  private func entry(_ key: Key) throws -> Value {
    guard let value = lookup[key.stringValue] else {
      throw CodingErrors.keyNotFound(key, codingPath: codingPath)
    }
    return value
  }

  private func decoder(for value: Value, key: any CodingKey) -> InternalValueDecoder {
    InternalValueDecoder(
      value: value, codingPath: codingPath.appending(key), userInfo: userInfo)
  }

  private func scalar<S: ValueScalarDecodable>(_ type: S.Type, _ key: Key) throws -> S {
    try S.decodedScalar(from: try entry(key), codingPath: codingPath.appending(key))
  }
}
