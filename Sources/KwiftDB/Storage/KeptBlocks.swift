import Foundation

/// Fixed-size cache of recently read sorted-table data blocks, kept decoded.
///
/// The slot mapping follows the storage specification: block `n` in table `t`
/// uses `(n + 61 * t) mod cachedBlocks`. A different block replaces the slot,
/// so cache eviction needs no linked list or timestamp bookkeeping. A block is
/// read, checked and decoded once when it enters its slot; a hit costs
/// nothing more than the slot lookup.
internal struct KeptBlocks {
  private struct Slot {
    var table: DatabaseUInt
    var block: Int
    var entries: [Entry]
  }

  private var slots: [Slot?] = Array(repeating: nil, count: Tuning.cachedBlocks)

  /// Returns a block's entries in table order, from its slot or read,
  /// checked, decoded and installed there.
  mutating func entries(
    table: DatabaseUInt,
    number: Int,
    handle: BlockHandle,
    file: StorageFile
  ) throws -> [Entry] {
    let tablePart = table.remainder(dividingBy: UInt64(Tuning.cachedBlocks))
    let slotIndex = (number % Tuning.cachedBlocks + 61 * Int(tablePart) % Tuning.cachedBlocks)
      % Tuning.cachedBlocks
    if let slot = slots[slotIndex], slot.table == table, slot.block == number {
      return slot.entries
    }

    let checkedLength = handle.length.addingReportingOverflow(4)
    guard handle.length >= 0, !checkedLength.overflow else {
      throw KwiftDBError.corruptStorage("data block length overflow")
    }
    let stored = try file.read(at: handle.offset, count: checkedLength.partialValue)
    let content = stored.prefix(handle.length)
    let checksumBytes = Array(stored.suffix(4))
    let expected = (UInt32(checksumBytes[0]) << 24)
      | (UInt32(checksumBytes[1]) << 16)
      | (UInt32(checksumBytes[2]) << 8)
      | UInt32(checksumBytes[3])
    guard CRC32C.checksum(content) == expected else {
      throw KwiftDBError.corruptStorage("data block checksum mismatch at offset \(handle.offset)")
    }
    let entries = try Self.decode(Data(content))
    slots[slotIndex] = Slot(table: table, block: number, entries: entries)
    return entries
  }

  private static func decode(_ bytes: Data) throws -> [Entry] {
    var reader = BinaryReader(bytes)
    var entries: [Entry] = []
    while !reader.isAtEnd {
      let entry = try TableCodec.readEntry(from: &reader)
      if let previous = entries.last,
         !previous.precedes(key: entry.key, sequence: entry.sequence) {
        throw KwiftDBError.corruptStorage("sorted-table data block is out of order")
      }
      entries.append(entry)
    }
    return entries
  }
}
