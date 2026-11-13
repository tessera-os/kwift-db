/// A stored key and the value visible at the scan's snapshot.
public struct KeyValue: Sendable, Equatable {
  /// The order-preserving key bytes.
  public var key: [UInt8]
  /// The encoded value bytes.
  public var value: [UInt8]

  /// Creates a key/value result returned by a database scan.
  public init(key: [UInt8], value: [UInt8]) {
    self.key = key
    self.value = value
  }
}
