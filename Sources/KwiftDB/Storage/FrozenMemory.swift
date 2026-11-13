/// A full memory table waiting to be written out as a level-0 table, with
/// the logs that hold its writes until then.
struct FrozenMemory: Sendable {
  var memory: MemoryTable
  /// Oldest first; removed once the table is written.
  var logs: [DatabaseUInt]
}
