/// Default size and count settings used by the KiraDB-compatible storage engine.
///
/// These values tune memory and I/O cost; they do not alter the file formats or
/// the visibility rules for a key. They are kept in one place so embedded and
/// server uses of KwiftDB share the same behavior.
internal enum Tuning {
  /// Flush the memory table after its measured bytes exceed four MiB.
  static let memoryTableBytes = 4_194_304
  /// The size compaction cuts its output tables at, so a later compaction
  /// rewrites only the tables whose keys it overlaps.
  static let tableBytes = 2_097_152
  /// Close a sorted-table data block once its entries reach four KiB.
  static let blockBytes = 4_096
  /// Keep the most recently checked 4 KiB blocks in the point-read cache.
  static let cachedBlocks = 2_048
  /// Allocate ten filter bits for each distinct key in a table.
  static let filterBitsPerKey = 10
  /// Set seven Bloom-filter bits per distinct key.
  static let filterProbes = 7
  /// Compact level zero when it contains four tables.
  static let levelZeroTables = 4
  /// Give level one a ten MiB target; deeper levels grow by a factor of ten.
  static let levelBytes: UInt64 = 10_485_760
  /// Use sixteen levels in the deterministic skip-list implementation.
  static let skipListLevels = 16
  /// Choose a higher skip-list level once in four draws.
  static let skipListBranching = 4
  /// Count this fixed overhead for every in-memory version entry.
  static let entryOverhead = 32
}
