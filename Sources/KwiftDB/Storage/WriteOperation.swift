import Foundation

/// One ordered mutation in an atomic storage batch.
public enum WriteOperation: Sendable, Equatable {
  /// Replaces the value stored at `key`.
  case put(key: [UInt8], value: [UInt8])
  /// Removes `key` if it currently has a value.
  case delete(key: [UInt8])

  /// The key affected by this mutation.
  public var key: [UInt8] {
    switch self {
    case .put(let key, _), .delete(let key): key
    }
  }
}
