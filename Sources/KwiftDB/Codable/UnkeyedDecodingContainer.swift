/// The unkeyed container: reads the elements of a `.list` in order.
///
/// An element error carries the element's position (`Index N`) in its coding
/// path. The position only advances when decoding an element succeeds, so a
/// failed `decode` can be retried with another type. Reading past the end
/// throws `DecodingError.valueNotFound`.
struct ValueUnkeyedDecodingContainer: UnkeyedDecodingContainer {
  let codingPath: [any CodingKey]
  let userInfo: [CodingUserInfoKey: Any]
  private let elements: [Value]
  private(set) var currentIndex = 0

  init(elements: [Value], codingPath: [any CodingKey], userInfo: [CodingUserInfoKey: Any]) {
    self.elements = elements
    self.codingPath = codingPath
    self.userInfo = userInfo
  }

  var count: Int? { elements.count }
  var isAtEnd: Bool { currentIndex >= elements.count }

  /// Returns true, and moves past the element, only if it is `.null`.
  mutating func decodeNil() throws -> Bool {
    let element = try peek(Never?.self)
    guard element == .null else { return false }
    currentIndex += 1
    return true
  }

  mutating func decode(_ type: Bool.Type) throws -> Bool { try scalar(type) }
  mutating func decode(_ type: String.Type) throws -> String { try scalar(type) }
  mutating func decode(_ type: Double.Type) throws -> Double { try scalar(type) }
  mutating func decode(_ type: Float.Type) throws -> Float { try scalar(type) }
  mutating func decode(_ type: Int.Type) throws -> Int { try scalar(type) }
  mutating func decode(_ type: Int8.Type) throws -> Int8 { try scalar(type) }
  mutating func decode(_ type: Int16.Type) throws -> Int16 { try scalar(type) }
  mutating func decode(_ type: Int32.Type) throws -> Int32 { try scalar(type) }
  mutating func decode(_ type: Int64.Type) throws -> Int64 { try scalar(type) }
  mutating func decode(_ type: UInt.Type) throws -> UInt { try scalar(type) }
  mutating func decode(_ type: UInt8.Type) throws -> UInt8 { try scalar(type) }
  mutating func decode(_ type: UInt16.Type) throws -> UInt16 { try scalar(type) }
  mutating func decode(_ type: UInt32.Type) throws -> UInt32 { try scalar(type) }
  mutating func decode(_ type: UInt64.Type) throws -> UInt64 { try scalar(type) }

  mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
    let element = try peek(type)
    let result = try decodeValue(
      type, from: element, codingPath: elementPath, userInfo: userInfo)
    currentIndex += 1
    return result
  }

  mutating func nestedContainer<NestedKey: CodingKey>(
    keyedBy type: NestedKey.Type
  ) throws -> KeyedDecodingContainer<NestedKey> {
    let element = try peek(KeyedDecodingContainer<NestedKey>.self)
    let result = try decoder(for: element).container(keyedBy: type)
    currentIndex += 1
    return result
  }

  mutating func nestedUnkeyedContainer() throws -> any UnkeyedDecodingContainer {
    let element = try peek((any UnkeyedDecodingContainer).self)
    let result = try decoder(for: element).unkeyedContainer()
    currentIndex += 1
    return result
  }

  /// A decoder over the next element.
  mutating func superDecoder() throws -> any Decoder {
    let element = try peek((any Decoder).self)
    let result = decoder(for: element)
    currentIndex += 1
    return result
  }

  private var elementPath: [any CodingKey] { codingPath.appending(index: currentIndex) }

  private func peek(_ type: Any.Type) throws -> Value {
    guard !isAtEnd else {
      throw CodingErrors.listExhausted(type, codingPath: elementPath)
    }
    return elements[currentIndex]
  }

  private func decoder(for value: Value) -> InternalValueDecoder {
    InternalValueDecoder(value: value, codingPath: elementPath, userInfo: userInfo)
  }

  private mutating func scalar<S: ValueScalarDecodable>(_ type: S.Type) throws -> S {
    let element = try peek(type)
    let result = try S.decodedScalar(from: element, codingPath: elementPath)
    currentIndex += 1
    return result
  }
}
