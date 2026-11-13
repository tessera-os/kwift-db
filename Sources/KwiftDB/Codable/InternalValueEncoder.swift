/// Encodes `value` into `node`, applying the special-type mapping first.
///
/// This is the single entry point every container and the public
/// `ValueEncoder` use for "encode an arbitrary `Encodable`".
func encodeValue<T: Encodable>(
  _ value: T, into node: EncodingNode, codingPath: [any CodingKey],
  userInfo: [CodingUserInfoKey: Any]
) throws {
  if let direct = try SpecialTypes.encode(value, codingPath: codingPath) {
    node.setLeaf(direct)
    return
  }
  let encoder = InternalValueEncoder(node: node, codingPath: codingPath, userInfo: userInfo)
  try value.encode(to: encoder)
}

/// The `Encoder` handed to `Encodable.encode(to:)`.
///
/// It writes into one `EncodingNode`. It is also its own
/// `SingleValueEncodingContainer`, since a single value container needs no
/// state beyond the node and path.
final class InternalValueEncoder: Encoder, SingleValueEncodingContainer {
  /// The node this encoder fills.
  let node: EncodingNode
  let codingPath: [any CodingKey]
  let userInfo: [CodingUserInfoKey: Any]

  init(node: EncodingNode, codingPath: [any CodingKey], userInfo: [CodingUserInfoKey: Any]) {
    self.node = node
    self.codingPath = codingPath
    self.userInfo = userInfo
  }

  func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
    node.becomeKeyed()
    return KeyedEncodingContainer(
      ValueKeyedEncodingContainer<Key>(node: node, codingPath: codingPath, userInfo: userInfo))
  }

  func unkeyedContainer() -> any UnkeyedEncodingContainer {
    node.becomeUnkeyed()
    return ValueUnkeyedEncodingContainer(node: node, codingPath: codingPath, userInfo: userInfo)
  }

  func singleValueContainer() -> any SingleValueEncodingContainer { self }

  // MARK: SingleValueEncodingContainer

  func encodeNil() throws { node.setLeaf(.null) }
  func encode(_ value: Bool) throws { try set(value) }
  func encode(_ value: String) throws { try set(value) }
  func encode(_ value: Double) throws { try set(value) }
  func encode(_ value: Float) throws { try set(value) }
  func encode(_ value: Int) throws { try set(value) }
  func encode(_ value: Int8) throws { try set(value) }
  func encode(_ value: Int16) throws { try set(value) }
  func encode(_ value: Int32) throws { try set(value) }
  func encode(_ value: Int64) throws { try set(value) }
  func encode(_ value: UInt) throws { try set(value) }
  func encode(_ value: UInt8) throws { try set(value) }
  func encode(_ value: UInt16) throws { try set(value) }
  func encode(_ value: UInt32) throws { try set(value) }
  func encode(_ value: UInt64) throws { try set(value) }

  func encode<T: Encodable>(_ value: T) throws {
    try encodeValue(value, into: node, codingPath: codingPath, userInfo: userInfo)
  }

  private func set<S: ValueScalarEncodable>(_ scalar: S) throws {
    node.setLeaf(try scalar.encodedScalar(codingPath: codingPath))
  }
}
