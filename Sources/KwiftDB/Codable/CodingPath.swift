/// A `CodingKey` for the path segments the coder invents itself.
///
/// Swift's `Codable` machinery reports where a failure happened as an array of
/// `CodingKey`s. Real keys come from the caller's types, but two kinds of
/// segment have no caller-defined key type: positions in an unkeyed container
/// (a list element) and the `super` slot used by `superEncoder()` and
/// `superDecoder()`. This type models both.
struct ValueCodingKey: CodingKey, Sendable {
  /// The textual form of the key. For list positions this is `Index N`.
  let stringValue: String
  /// The list position, or `nil` for a named key.
  let intValue: Int?

  /// Creates a named key.
  init(stringValue: String) {
    self.stringValue = stringValue
    self.intValue = nil
  }

  /// Creates a key from an integer; it reads as a list position.
  init(intValue: Int) {
    self.stringValue = "Index \(intValue)"
    self.intValue = intValue
  }

  /// Creates the key for the element at `index` of a list.
  init(index: Int) {
    self.init(intValue: index)
  }

  /// The key `superEncoder()` and `superDecoder()` use: `super`.
  static let `super` = ValueCodingKey(stringValue: "super")
}

extension Array where Element == any CodingKey {
  /// Returns this coding path with `key` appended.
  ///
  /// Coding paths are value arrays, so every container derives the path of its
  /// children by appending to its own.
  func appending(_ key: any CodingKey) -> [any CodingKey] {
    var copy = self
    copy.append(key)
    return copy
  }

  /// Returns this coding path with the list position `index` appended.
  func appending(index: Int) -> [any CodingKey] {
    appending(ValueCodingKey(index: index))
  }
}
