/// The unkeyed container: encodes into a list.
///
/// Element errors carry the element's position (`Index N`) in their coding path.
struct ValueUnkeyedEncodingContainer: UnkeyedEncodingContainer {
  let node: EncodingNode
  let codingPath: [any CodingKey]
  let userInfo: [CodingUserInfoKey: Any]

  /// The number of elements encoded so far.
  var count: Int { node.count }

  private var nextPath: [any CodingKey] { codingPath.appending(index: node.count) }

  mutating func encodeNil() throws { node.appendChild().setLeaf(.null) }

  mutating func encode(_ value: Bool) throws { try append(value) }
  mutating func encode(_ value: String) throws { try append(value) }
  mutating func encode(_ value: Double) throws { try append(value) }
  mutating func encode(_ value: Float) throws { try append(value) }
  mutating func encode(_ value: Int) throws { try append(value) }
  mutating func encode(_ value: Int8) throws { try append(value) }
  mutating func encode(_ value: Int16) throws { try append(value) }
  mutating func encode(_ value: Int32) throws { try append(value) }
  mutating func encode(_ value: Int64) throws { try append(value) }
  mutating func encode(_ value: UInt) throws { try append(value) }
  mutating func encode(_ value: UInt8) throws { try append(value) }
  mutating func encode(_ value: UInt16) throws { try append(value) }
  mutating func encode(_ value: UInt32) throws { try append(value) }
  mutating func encode(_ value: UInt64) throws { try append(value) }

  mutating func encode<T: Encodable>(_ value: T) throws {
    let child = EncodingNode()
    try encodeValue(value, into: child, codingPath: nextPath, userInfo: userInfo)
    node.appendChild().adopt(child)
  }

  mutating func nestedContainer<NestedKey: CodingKey>(
    keyedBy keyType: NestedKey.Type
  ) -> KeyedEncodingContainer<NestedKey> {
    let path = nextPath
    let child = node.appendChild()
    child.becomeKeyed()
    return KeyedEncodingContainer(
      ValueKeyedEncodingContainer<NestedKey>(node: child, codingPath: path, userInfo: userInfo))
  }

  mutating func nestedUnkeyedContainer() -> any UnkeyedEncodingContainer {
    let path = nextPath
    let child = node.appendChild()
    child.becomeUnkeyed()
    return ValueUnkeyedEncodingContainer(node: child, codingPath: path, userInfo: userInfo)
  }

  /// An encoder for a new element at the end of the list.
  mutating func superEncoder() -> any Encoder {
    let path = nextPath
    return InternalValueEncoder(node: node.appendChild(), codingPath: path, userInfo: userInfo)
  }

  private mutating func append<S: ValueScalarEncodable>(_ scalar: S) throws {
    let value = try scalar.encodedScalar(codingPath: nextPath)
    node.appendChild().setLeaf(value)
  }
}
