import Foundation

/// One immutable sorted table, with its Bloom filter and index held in memory.
///
/// Opening a table reads only its footer, filter, and index. Data blocks are
/// fetched on demand and checked against their CRC-32C before entering the
/// shared fixed-size block cache.
internal final class SortedTable {
  let record: TableRecord
  let size: UInt64
  private let file: StorageFile
  private let filter: BloomFilter
  private let handles: [BlockHandle]

  private init(record: TableRecord, size: UInt64, file: StorageFile, filter: BloomFilter, handles: [BlockHandle]) {
    self.record = record
    self.size = size
    self.file = file
    self.filter = filter
    self.handles = handles
  }

  /// Opens a table and reads the footer, filter, and block index.
  static func open(record: TableRecord, at url: URL) throws -> SortedTable {
    let file = try StorageFile.openForReading(at: url)
    do {
      let size = try file.size()
      guard size >= UInt64(TableCodec.footerBytes) else {
        throw KwiftDBError.corruptStorage("table is shorter than its footer")
      }
      let footerOffset = size - UInt64(TableCodec.footerBytes)
      let footerData = try file.read(at: footerOffset, count: TableCodec.footerBytes)
      var footer = BinaryReader(footerData)
      let filterOffset = UInt64(try footer.readBigEndianUInt32())
      let filterLength = Int(try footer.readBigEndianUInt32())
      let indexOffset = UInt64(try footer.readBigEndianUInt32())
      let indexLength = Int(try footer.readBigEndianUInt32())
      let magic = try footer.readBytes(count: TableCodec.magic.count)
      guard footer.isAtEnd, magic == TableCodec.magic else {
        throw KwiftDBError.corruptStorage("table footer magic or length is invalid")
      }

      let filterEnd = try checkedEnd(offset: filterOffset, length: filterLength)
      let indexEnd = try checkedEnd(offset: indexOffset, length: indexLength)
      guard filterEnd == indexOffset, indexEnd == footerOffset else {
        throw KwiftDBError.corruptStorage("table footer ranges are inconsistent")
      }
      let filterData = try readCheckedBlock(file, offset: filterOffset, length: filterLength)
      let filter = try BloomFilter.decode(filterData)
      let indexData = try readCheckedBlock(file, offset: indexOffset, length: indexLength)
      var indexReader = BinaryReader(indexData)
      var handles: [BlockHandle] = []
      while !indexReader.isAtEnd {
        let handle = try TableCodec.readBlockHandle(from: &indexReader)
        guard handle.length > 0, handle.offset < filterOffset else {
          throw KwiftDBError.corruptStorage("table index points outside its data blocks")
        }
        handles.append(handle)
      }
      guard !handles.isEmpty else { throw KwiftDBError.corruptStorage("table has no data blocks") }
      for pair in zip(handles, handles.dropFirst()) {
        guard !pair.1.lastKey.lexicographicallyPrecedes(pair.0.lastKey) else {
          throw KwiftDBError.corruptStorage("table index keys are out of order")
        }
      }
      return SortedTable(record: record, size: size, file: file, filter: filter, handles: handles)
    } catch {
      try? file.close()
      throw error
    }
  }

  /// Writes sorted entries into a new table, including checksums and footer.
  ///
  /// The caller supplies entries ordered by key ascending and sequence
  /// descending. The returned manifest record uses the first and last keys in
  /// that order. The file is synchronized before this method returns.
  static func write(
    entries: [Entry],
    level: Int,
    number: DatabaseUInt,
    at url: URL
  ) throws -> (record: TableRecord, size: UInt64) {
    guard let first = entries.first, let last = entries.last else {
      throw KwiftDBError.io("cannot write an empty sorted table")
    }
    let file = try StorageFile.createNew(at: url)
    do {
      var handles: [BlockHandle] = []
      var keys: [[UInt8]] = []
      var block = Data()

      for entry in entries {
        TableCodec.append(entry, to: &block)
        if keys.last != entry.key { keys.append(entry.key) }
        if block.count >= Tuning.blockBytes {
          try appendDataBlock(block, lastKey: entry.key, to: file, handles: &handles)
          block.removeAll(keepingCapacity: true)
        }
      }
      if !block.isEmpty {
        try appendDataBlock(block, lastKey: last.key, to: file, handles: &handles)
      }

      let filter = try BloomFilter.build(keys: keys)
      let filterBlock = filter.encode()
      let filterOffset = try file.append(checked(filterBlock))
      let indexOffset = try file.size()
      var indexBlock = Data()
      for handle in handles { TableCodec.append(handle, to: &indexBlock) }
      let storedIndexOffset = try file.append(checked(indexBlock))
      guard storedIndexOffset == indexOffset else {
        throw KwiftDBError.io("sorted-table index offset changed unexpectedly")
      }

      let footerValues = [
        filterOffset,
        UInt64(filterBlock.count),
        indexOffset,
        UInt64(indexBlock.count),
      ]
      guard footerValues.allSatisfy({ $0 <= UInt64(UInt32.max) }) else {
        throw KwiftDBError.io("sorted table exceeds the four-byte footer range")
      }
      var footer = Data()
      for value in footerValues { BinaryWriter.appendBigEndian(UInt32(value), to: &footer) }
      footer.append(contentsOf: TableCodec.magic)
      _ = try file.append(footer)
      try file.synchronize()
      let size = try file.size()
      try file.close()

      return (
        TableRecord(level: level, number: number, smallest: first.key, largest: last.key),
        size
      )
    } catch {
      try? file.close()
      throw error
    }
  }

  /// Returns the newest version of `key` visible at `snapshot`.
  func find(
    _ key: [UInt8],
    at snapshot: DatabaseUInt,
    cache: inout KeptBlocks
  ) throws -> Version? {
    guard filter.mayContain(key) else { return nil }
    var index = firstBlock(whoseLastKeyIsAtLeast: key)
    while index < handles.count {
      let handle = handles[index]
      let entries = try readEntries(in: index, cache: &cache)
      for entry in entries[Self.firstEntry(atLeast: key, in: entries)...] {
        guard entry.key == key else { return nil }
        if entry.sequence <= snapshot { return entry.version }
      }
      if handle.lastKey != key { return nil }
      index += 1
    }
    return nil
  }

  /// Returns all key versions in the half-open range `[start, end)`.
  func entries(
    from start: [UInt8],
    to end: [UInt8],
    cache: inout KeptBlocks
  ) throws -> [Entry] {
    guard start.lexicographicallyPrecedes(end) else { return [] }
    var result: [Entry] = []
    var index = firstBlock(whoseLastKeyIsAtLeast: start)
    while index < handles.count {
      let handle = handles[index]
      let entries = try readEntries(in: index, cache: &cache)
      for entry in entries[Self.firstEntry(atLeast: start, in: entries)...] {
        guard entry.key.lexicographicallyPrecedes(end) else { break }
        result.append(entry)
      }
      if !handle.lastKey.lexicographicallyPrecedes(end) { break }
      index += 1
    }
    return result
  }

  /// The number of data blocks, which ``entries(inBlock:cache:)`` reads one at a time.
  var blockCount: Int { handles.count }

  /// Reads one data block's entries in table order, for merges that stream.
  func entries(inBlock number: Int, cache: inout KeptBlocks) throws -> [Entry] {
    try readEntries(in: number, cache: &cache)
  }

  /// Reads every entry in table order.
  func allEntries(cache: inout KeptBlocks) throws -> [Entry] {
    var result: [Entry] = []
    for index in handles.indices {
      result.append(contentsOf: try readEntries(in: index, cache: &cache))
    }
    return result
  }

  /// Closes the table's read handle after it leaves the manifest.
  func close() throws { try file.close() }

  private func firstBlock(whoseLastKeyIsAtLeast key: [UInt8]) -> Int {
    var lower = 0
    var upper = handles.count
    while lower < upper {
      let middle = lower + (upper - lower) / 2
      if handles[middle].lastKey.lexicographicallyPrecedes(key) {
        lower = middle + 1
      } else {
        upper = middle
      }
    }
    return lower
  }

  private func readEntries(in number: Int, cache: inout KeptBlocks) throws -> [Entry] {
    try cache.entries(table: record.number, number: number, handle: handles[number], file: file)
  }

  /// The index of the first entry in `entries` whose key is not before `key`.
  private static func firstEntry(atLeast key: [UInt8], in entries: [Entry]) -> Int {
    var lower = 0
    var upper = entries.count
    while lower < upper {
      let middle = lower + (upper - lower) / 2
      if entries[middle].key.lexicographicallyPrecedes(key) { lower = middle + 1 } else { upper = middle }
    }
    return lower
  }

  private static func appendDataBlock(
    _ block: Data,
    lastKey: [UInt8],
    to file: StorageFile,
    handles: inout [BlockHandle]
  ) throws {
    let offset = try file.append(checked(block))
    handles.append(BlockHandle(lastKey: lastKey, offset: offset, length: block.count))
  }

  private static func checked(_ block: Data) -> Data {
    var stored = block
    CRC32C.append(CRC32C.checksum(block), to: &stored)
    return stored
  }

  private static func readCheckedBlock(_ file: StorageFile, offset: UInt64, length: Int) throws -> Data {
    let (storedLength, overflow) = length.addingReportingOverflow(4)
    guard length >= 0, !overflow else { throw KwiftDBError.corruptStorage("table block length overflow") }
    let stored = try file.read(at: offset, count: storedLength)
    let content = stored.prefix(length)
    let checksum = Array(stored.suffix(4))
    let expected = (UInt32(checksum[0]) << 24)
      | (UInt32(checksum[1]) << 16)
      | (UInt32(checksum[2]) << 8)
      | UInt32(checksum[3])
    guard CRC32C.checksum(content) == expected else {
      throw KwiftDBError.corruptStorage("table block checksum mismatch at offset \(offset)")
    }
    return Data(content)
  }

  private static func checkedEnd(offset: UInt64, length: Int) throws -> UInt64 {
    guard length >= 0 else { throw KwiftDBError.corruptStorage("negative table block length") }
    let (withBlock, blockOverflow) = offset.addingReportingOverflow(UInt64(length))
    let (end, checksumOverflow) = withBlock.addingReportingOverflow(4)
    guard !blockOverflow, !checksumOverflow else {
      throw KwiftDBError.corruptStorage("table block range overflow")
    }
    return end
  }

}
