/// The manifest entry for one immutable sorted table.
internal struct TableRecord: Sendable, Equatable {
  /// The leveled-compaction level that owns the table.
  var level: Int
  /// The never-reused file number that names the table.
  var number: DatabaseUInt
  /// The smallest key in the table, inclusive.
  var smallest: [UInt8]
  /// The largest key in the table, inclusive.
  var largest: [UInt8]

  /// Whether this table's inclusive key range contains `key`.
  func covers(_ key: [UInt8]) -> Bool {
    !key.lexicographicallyPrecedes(smallest) && !largest.lexicographicallyPrecedes(key)
  }

  /// Whether this table intersects an inclusive key range.
  func overlaps(_ lower: [UInt8], _ upper: [UInt8]) -> Bool {
    !upper.lexicographicallyPrecedes(smallest) && !largest.lexicographicallyPrecedes(lower)
  }
}
