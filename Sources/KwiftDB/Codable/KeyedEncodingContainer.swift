/// The keyed container: encodes into a document, fields in encoding order.
///
/// `encodeIfPresent(nil)` (and therefore optional properties of synthesized
/// `Encodable` types) omits the field entirely. Encoding an explicit `nil`
/// through `encodeNil(forKey:)`, or a non-`IfPresent` `encode` of an `Optional`
/// that is `nil`, writes `.null`.
struct ValueKeyedEncodingContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
  let node: EncodingNode
  let codingPath: [any CodingKey]
  let userInfo: [CodingUserInfoKey: Any]

  mutating func encodeNil(forKey key: Key) throws {
    node.child(forKey: key.stringValue).setLeaf(.null)
  }

  mutating func encode(_ value: Bool, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: String, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: Double, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: Float, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: Int, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: Int8, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: Int16, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: Int32, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: Int64, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: UInt, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: UInt8, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: UInt16, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: UInt32, forKey key: Key) throws { try set(value, key) }
  mutating func encode(_ value: UInt64, forKey key: Key) throws { try set(value, key) }

  mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
    // Encode into a detached node first so a failure leaves no half-written field.
    let child = EncodingNode()
    try encodeValue(
      value, into: child, codingPath: codingPath.appending(key), userInfo: userInfo)
    node.child(forKey: key.stringValue).adopt(child)
  }

  mutating func nestedContainer<NestedKey: CodingKey>(
    keyedBy keyType: NestedKey.Type, forKey key: Key
  ) -> KeyedEncodingContainer<NestedKey> {
    let child = node.child(forKey: key.stringValue)
    child.becomeKeyed()
    return KeyedEncodingContainer(
      ValueKeyedEncodingContainer<NestedKey>(
        node: child, codingPath: codingPath.appending(key), userInfo: userInfo))
  }

  mutating func nestedUnkeyedContainer(forKey key: Key) -> any UnkeyedEncodingContainer {
    let child = node.child(forKey: key.stringValue)
    child.becomeUnkeyed()
    return ValueUnkeyedEncodingContainer(
      node: child, codingPath: codingPath.appending(key), userInfo: userInfo)
  }

  /// An encoder for the field named `super`.
  mutating func superEncoder() -> any Encoder {
    makeSuperEncoder(ValueCodingKey.super)
  }

  /// An encoder for the field named by `key`.
  mutating func superEncoder(forKey key: Key) -> any Encoder {
    makeSuperEncoder(key)
  }

  private func makeSuperEncoder(_ key: any CodingKey) -> any Encoder {
    InternalValueEncoder(
      node: node.child(forKey: key.stringValue), codingPath: codingPath.appending(key),
      userInfo: userInfo)
  }

  private func set<S: ValueScalarEncodable>(_ scalar: S, _ key: Key) throws {
    let value = try scalar.encodedScalar(codingPath: codingPath.appending(key))
    node.child(forKey: key.stringValue).setLeaf(value)
  }
}
