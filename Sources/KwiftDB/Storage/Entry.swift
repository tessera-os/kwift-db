/// One key version in the memory table or a sorted table.
internal struct Entry: Sendable, Equatable {
  /// The order-preserving key bytes.
  var key: [UInt8]
  /// The write sequence that introduced this version.
  var sequence: DatabaseUInt
  /// The put value or deletion marker.
  var version: Version

  /// Whether this entry sorts before the supplied key and sequence pair.
  ///
  /// Keys sort ascending. Versions of one key sort by sequence descending so
  /// the newest version is encountered first during a read.
  func precedes(key otherKey: [UInt8], sequence otherSequence: DatabaseUInt) -> Bool {
    if key != otherKey { return key.lexicographicallyPrecedes(otherKey) }
    return sequence > otherSequence
  }
}
