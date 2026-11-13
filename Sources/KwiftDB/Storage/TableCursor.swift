/// Reads one sorted table's entries in order, a block at a time.
struct TableCursor {
  let table: SortedTable
  private var block = 0
  private var entries: [Entry] = []
  private var index = 0

  init(_ table: SortedTable) { self.table = table }

  /// The entry under the cursor, or nil past the last one.
  mutating func current(cache: inout KeptBlocks) throws -> Entry? {
    while index >= entries.count {
      guard block < table.blockCount else { return nil }
      entries = try table.entries(inBlock: block, cache: &cache)
      block += 1
      index = 0
    }
    return entries[index]
  }

  mutating func advance() { index += 1 }
}
