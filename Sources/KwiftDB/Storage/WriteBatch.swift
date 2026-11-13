/// An ordered group of key changes committed as one database sequence.
///
/// KwiftDB appends a complete batch to its write-ahead log and syncs that log
/// before the write is acknowledged. If a key is added more than once, its
/// final operation replaces the earlier operation while retaining the key's
/// original position in the batch.
public struct WriteBatch: Sendable, Equatable {
  /// Mutations in their stable batch order.
  public private(set) var operations: [WriteOperation]
  /// Where each key's operation sits in ``operations``, so replacing or
  /// finding one does not scan the batch.
  private var positions: [[UInt8]: Int] = [:]

  /// Creates an empty batch.
  public init() { operations = [] }

  /// Creates a batch from operations, in order; a later operation on a key
  /// replaces an earlier one.
  public init(_ operations: [WriteOperation]) {
    self.operations = []
    for operation in operations {
      switch operation {
      case .put(let key, let value): put(value, for: key)
      case .delete(let key): delete(key)
      }
    }
  }

  /// Whether this batch contains no mutations.
  public var isEmpty: Bool { operations.isEmpty }

  /// Adds or replaces a value in this batch.
  ///
  /// - Parameters:
  ///   - value: The encoded value bytes to store.
  ///   - key: The order-preserving key bytes to update.
  public mutating func put(_ value: [UInt8], for key: [UInt8]) {
    replace(WriteOperation.put(key: key, value: value), for: key)
  }

  /// Adds or replaces a deletion in this batch.
  ///
  /// - Parameter key: The order-preserving key bytes to remove.
  public mutating func delete(_ key: [UInt8]) {
    replace(WriteOperation.delete(key: key), for: key)
  }

  /// The batch's operation on `key`, or nil when it has none.
  public func operation(for key: [UInt8]) -> WriteOperation? {
    positions[key].map { operations[$0] }
  }

  public static func == (lhs: WriteBatch, rhs: WriteBatch) -> Bool {
    lhs.operations == rhs.operations
  }

  private mutating func replace(_ operation: WriteOperation, for key: [UInt8]) {
    if let index = positions[key] {
      operations[index] = operation
    } else {
      positions[key] = operations.count
      operations.append(operation)
    }
  }
}
