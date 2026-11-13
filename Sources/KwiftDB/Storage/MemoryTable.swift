/// The ordered in-memory index of writes that have not yet been flushed.
///
/// Entries remain ordered by key ascending and, for the same key, sequence
/// descending. The implementation uses binary insertion into an array, which
/// keeps the ordering explicit and lets snapshots use the same read rules as
/// sorted tables. The measured size follows KiraDB's fixed 32-byte entry
/// overhead plus the key and put-value byte counts.
internal struct MemoryTable: Sendable {
  private(set) var entries: [Entry] = []
  private(set) var bytes = 0

  /// Inserts every operation in one committed batch at its assigned sequence.
  mutating func insert(_ batch: SequencedBatch) {
    for operation in batch.operations {
      switch operation {
      case .put(let key, let value):
        insert(Entry(key: key, sequence: batch.sequence, version: .put(value)))
      case .delete(let key):
        insert(Entry(key: key, sequence: batch.sequence, version: .delete))
      }
    }
  }

  /// Inserts one version at its sorted position.
  mutating func insert(_ entry: Entry) {
    bytes += entry.key.count + entry.version.valueByteCount + Tuning.entryOverhead
    var lower = 0
    var upper = entries.count
    while lower < upper {
      let middle = lower + (upper - lower) / 2
      if entries[middle].precedes(key: entry.key, sequence: entry.sequence) {
        lower = middle + 1
      } else {
        upper = middle
      }
    }
    entries.insert(entry, at: lower)
  }

  /// Finds the newest version of `key` that is visible at `snapshot`.
  func find(_ key: [UInt8], at snapshot: DatabaseUInt) -> Version? {
    var index = lowerBound(for: key)
    while index < entries.count && entries[index].key == key {
      if entries[index].sequence <= snapshot { return entries[index].version }
      index += 1
    }
    return nil
  }

  /// Returns all entries in the half-open key range visible at `snapshot`.
  func entries(from start: [UInt8], to end: [UInt8], at snapshot: DatabaseUInt) -> [Entry] {
    guard start.lexicographicallyPrecedes(end) else { return [] }
    var result: [Entry] = []
    var index = lowerBound(for: start)
    while index < entries.count {
      let entry = entries[index]
      guard entry.key.lexicographicallyPrecedes(end) else { break }
      if entry.sequence <= snapshot { result.append(entry) }
      index += 1
    }
    return result
  }

  /// Resets the table after its entries have been durably written as a table.
  mutating func removeAll() {
    entries.removeAll(keepingCapacity: true)
    bytes = 0
  }

  private func lowerBound(for key: [UInt8]) -> Int {
    var lower = 0
    var upper = entries.count
    while lower < upper {
      let middle = lower + (upper - lower) / 2
      if entries[middle].key.lexicographicallyPrecedes(key) {
        lower = middle + 1
      } else {
        upper = middle
      }
    }
    return lower
  }
}
