/// The value a stored key has at one sequence number.
internal enum Version: Sendable, Equatable {
  /// The key was assigned these encoded value bytes.
  case put([UInt8])
  /// The key was deleted at this sequence.
  case delete

  /// The number of value bytes counted toward the memory-table threshold.
  var valueByteCount: Int {
    switch self {
    case .put(let bytes): bytes.count
    case .delete: 0
    }
  }
}
