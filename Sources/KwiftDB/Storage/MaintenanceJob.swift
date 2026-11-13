import Foundation

/// One piece of storage upkeep: a frozen memory table to write out, or tables
/// to merge one level down.
///
/// The store plans a job from its state, the job writes its new tables from
/// inputs that no longer change (a frozen memory table, immutable table files),
/// and the store installs the result. Writing is the expensive part, and it
/// needs nothing from the store, so it runs off the store's actor while reads
/// and writes go on.
struct MaintenanceJob: Sendable {
  enum Work: Sendable {
    /// Writes a frozen memory table as one level-0 table.
    case flush(MemoryTable)
    /// Merges `inputs` into tables at the job's level, keeping each key's
    /// newest version and, when `dropsDeletions`, dropping deletions that no
    /// deeper table needs to hide anything.
    case compact(inputs: [TableRecord], dropsDeletions: Bool)
  }

  let work: Work
  /// The level the new tables join.
  let level: Int
  /// File numbers reserved for the new tables, used in order.
  let numbers: [DatabaseUInt]
  let directory: URL

  /// Writes the job's tables, synchronized, and returns them.
  func run() throws -> [WrittenTable] {
    switch work {
    case .flush(let memory):
      guard !memory.entries.isEmpty, let number = numbers.first else { return [] }
      return [try write(memory.entries, number: number)]
    case .compact(let inputs, let dropsDeletions):
      return try compact(inputs, dropsDeletions: dropsDeletions)
    }
  }

  /// Runs ``run()`` off the caller's actor.
  @concurrent func runConcurrently() async throws -> [WrittenTable] { try run() }

  private func compact(_ inputs: [TableRecord], dropsDeletions: Bool) throws -> [WrittenTable] {
    let tables = try inputs.map { try SortedTable.open(record: $0, at: Store.tableURL(directory, number: $0.number)) }
    defer { for table in tables { try? table.close() } }
    var cursors = tables.map(TableCursor.init)
    var cache = KeptBlocks()
    var written: [WrittenTable] = []
    var pending: [Entry] = []
    var pendingBytes = 0
    var nextNumber = numbers.startIndex

    func cut() throws {
      guard !pending.isEmpty else { return }
      // The reservation covers the expected output; a larger one keeps
      // writing into its last table rather than running out of names.
      let number = numbers[min(nextNumber, numbers.count - 1)]
      written.append(try write(pending, number: number))
      nextNumber += 1
      pending.removeAll(keepingCapacity: true)
      pendingBytes = 0
    }

    // Merge by key ascending, newest version first; the first version of a
    // key is the one to keep.
    var lastKey: [UInt8]?
    while let index = try Self.smallest(&cursors, cache: &cache) {
      guard let entry = try cursors[index].current(cache: &cache) else { break }
      cursors[index].advance()
      if entry.key == lastKey { continue }
      lastKey = entry.key
      if case .delete = entry.version, dropsDeletions { continue }
      // Cut only between keys, and never in the last reserved table.
      if pendingBytes >= Tuning.tableBytes, nextNumber < numbers.count - 1 { try cut() }
      pending.append(entry)
      pendingBytes += entry.key.count + entry.version.valueByteCount + Tuning.entryOverhead
    }
    try cut()
    return written
  }

  /// The cursor whose current entry comes first, or nil when all are done.
  private static func smallest(_ cursors: inout [TableCursor], cache: inout KeptBlocks) throws -> Int? {
    var best: Int?
    var bestEntry: Entry?
    for index in cursors.indices {
      guard let entry = try cursors[index].current(cache: &cache) else { continue }
      if let current = bestEntry, !entry.precedes(key: current.key, sequence: current.sequence) { continue }
      best = index
      bestEntry = entry
    }
    return best
  }

  private func write(_ entries: [Entry], number: DatabaseUInt) throws -> WrittenTable {
    let url = Store.tableURL(directory, number: number)
    let artifact = try SortedTable.write(entries: entries, level: level, number: number, at: url)
    return WrittenTable(record: artifact.record, size: artifact.size)
  }
}
