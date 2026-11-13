import Foundation

/// An ordered key/value store backed by a write-ahead log and sorted tables.
///
/// `Store` is the bottom layer of KwiftDB. Each successful non-empty batch is
/// assigned one sequence number, written and synchronized to the active log,
/// then inserted into the memory table. Large memory tables flush to immutable
/// sorted tables and are compacted through levels. Reads observe one sequence
/// snapshot and hide deletions and older versions.
///
/// A write only appends to the log and the memory table. A full memory table
/// is frozen behind a new log, and the frozen tables are written out and
/// compacted by ``maintain()``, which does the writing off the actor so reads
/// and writes never wait for it. Without a running `maintain()`, a write that
/// freezes a memory table does that upkeep itself.
///
/// Separate `Store` instances must not open the same directory at once.
public actor Store {
  let directory: URL
  var manifest: Manifest
  private var log: LogWriter
  private var memory: MemoryTable
  /// The logs that hold the memory table's writes, oldest first.
  private var memoryLogs: [DatabaseUInt]
  /// Full memory tables waiting to be written out, oldest first.
  var frozen: [FrozenMemory] = []
  var tables: [SortedTable]
  var cache = KeptBlocks()
  private var closed = false
  /// Wakes the running ``maintain()``; nil when none runs.
  var maintenanceWake: AsyncStream<Void>.Continuation?
  /// Where each level's next compaction starts, so levels compact round-robin.
  var compactionPointers: [Int: [UInt8]] = [:]

  /// Opens an existing database directory or creates a new one.
  ///
  /// Recovery loads the manifest, opens its immutable tables, replays every
  /// complete checksummed record in the active log, and truncates any torn
  /// unacknowledged tail before new writes can be appended.
  ///
  /// - Parameter directory: The directory that owns `MANIFEST`, numbered logs,
  ///   and numbered sorted-table files.
  /// - Throws: `KwiftDBError.io` for file-system failures or
  ///   `KwiftDBError.corruptStorage` when a referenced file cannot be decoded.
  public init(directory: URL) throws {
    let manager = FileManager.default
    var isDirectory: ObjCBool = false
    if manager.fileExists(atPath: directory.path, isDirectory: &isDirectory) {
      guard isDirectory.boolValue else {
        throw KwiftDBError.io("database path is not a directory: \(directory.path)")
      }
    } else {
      do { try manager.createDirectory(at: directory, withIntermediateDirectories: true) }
      catch { throw KwiftDBError.io(error.localizedDescription) }
    }

    let manifestURL = directory.appendingPathComponent(ManifestFile.name, isDirectory: false)
    if !manager.fileExists(atPath: manifestURL.path) {
      let freshManifest = ManifestFile.fresh()
      let logURL = Self.logURL(directory, number: freshManifest.log)
      if manager.fileExists(atPath: logURL.path) {
        do { try manager.removeItem(at: logURL) }
        catch { throw KwiftDBError.io(error.localizedDescription) }
      }
      let freshLog = try LogWriter.create(at: logURL)
      do { try ManifestFile.store(freshManifest, in: directory) }
      catch {
        try? freshLog.close()
        try? manager.removeItem(at: logURL)
        throw error
      }
      self.directory = directory
      manifest = freshManifest
      log = freshLog
      memory = MemoryTable()
      memoryLogs = [freshManifest.log]
      tables = []
      return
    }

    var recoveredManifest = try ManifestFile.load(from: directory)
    var recoveredTables: [SortedTable] = []
    do {
      for record in recoveredManifest.tables {
        recoveredTables.append(
          try SortedTable.open(record: record, at: Self.tableURL(directory, number: record.number))
        )
      }
    } catch {
      for table in recoveredTables { try? table.close() }
      throw error
    }

    // Every log from the manifest's on holds writes no table has yet: the
    // memory table's and those of frozen tables not written out.
    let logNumbers: [DatabaseUInt]
    do { logNumbers = try Self.liveLogs(in: directory, from: recoveredManifest.log) }
    catch {
      for table in recoveredTables { try? table.close() }
      throw error
    }
    var recoveredMemory = MemoryTable()
    let recoveredWriter: LogWriter
    do {
      var lastValidBytes: UInt64 = 0
      for number in logNumbers {
        let read = try LogReader.read(at: Self.logURL(directory, number: number))
        for batch in read.batches {
          recoveredMemory.insert(batch)
          if recoveredManifest.lastSequence < batch.sequence {
            recoveredManifest.lastSequence = batch.sequence
          }
        }
        lastValidBytes = read.validByteCount
      }
      recoveredWriter = try LogWriter.openForAppend(
        at: Self.logURL(directory, number: logNumbers.last ?? recoveredManifest.log),
        validByteCount: lastValidBytes
      )
    } catch {
      for table in recoveredTables { try? table.close() }
      throw error
    }

    // A crash can leave a fully or partly written table/log whose allocation
    // was never committed to MANIFEST. Discover those names before allocating
    // again so a durable file number is never reused, then remove files the
    // installed manifest does not own.
    try Self.reconcileFiles(in: directory, manifest: &recoveredManifest)

    self.directory = directory
    manifest = recoveredManifest
    log = recoveredWriter
    memory = recoveredMemory
    memoryLogs = logNumbers.isEmpty ? [recoveredManifest.log] : logNumbers
    tables = Self.newestFirst(recoveredTables)
  }

  /// Reads the value visible for `key` at the current store sequence.
  ///
  /// - Parameter key: The order-preserving key bytes.
  /// - Returns: Encoded value bytes, or `nil` when the key is absent or deleted.
  /// - Throws: `KwiftDBError` if a table block cannot be read or verified.
  public func get(_ key: [UInt8]) throws -> [UInt8]? {
    try ensureOpen()
    let snapshot = manifest.lastSequence
    if let version = memory.find(key, at: snapshot) { return value(of: version) }
    for held in frozen.reversed() {
      if let version = held.memory.find(key, at: snapshot) { return value(of: version) }
    }
    for table in tables where table.record.covers(key) {
      if let version = try table.find(key, at: snapshot, cache: &cache) {
        return value(of: version)
      }
    }
    return nil
  }

  /// The last committed sequence, used by synchronization transitions.
  public func version() throws -> Number {
    try ensureOpen()
    return try Number(manifest.lastSequence.description)
  }

  /// Reads visible key/value pairs in the half-open key range `[start, end)`.
  ///
  /// Versions from memory and all overlapping sorted tables are merged by key
  /// ascending and sequence descending. Only the newest version at the read's
  /// captured sequence is returned; tombstones are omitted.
  ///
  /// - Parameters:
  ///   - start: The inclusive lower key bound.
  ///   - end: The exclusive upper key bound.
  /// - Returns: Visible values in key order.
  /// - Throws: `KwiftDBError` if a relevant table block is corrupt or unreadable.
  public func scan(from start: [UInt8], to end: [UInt8]) throws -> [KeyValue] {
    try ensureOpen()
    guard start.lexicographicallyPrecedes(end) else { return [] }
    let snapshot = manifest.lastSequence
    var entries = memory.entries(from: start, to: end, at: snapshot)
    for held in frozen { entries.append(contentsOf: held.memory.entries(from: start, to: end, at: snapshot)) }
    for table in tables where table.record.overlaps(start, end) {
      entries.append(contentsOf: try table.entries(from: start, to: end, cache: &cache))
    }
    entries.sort(by: Self.entryPrecedes)

    var result: [KeyValue] = []
    var index = 0
    while index < entries.count {
      let key = entries[index].key
      var visible: Entry?
      while index < entries.count && entries[index].key == key {
        if visible == nil && entries[index].sequence <= snapshot { visible = entries[index] }
        index += 1
      }
      if let visible, case .put(let value) = visible.version {
        result.append(KeyValue(key: key, value: value))
      }
    }
    return result
  }

  /// Reads every block of every sorted table, so a corrupt table is found
  /// now rather than by the first read that reaches it.
  ///
  /// - Throws: `KwiftDBError.corruptStorage` for a block whose checksum or
  ///   order is wrong.
  public func verify() throws {
    try ensureOpen()
    for table in tables { _ = try table.allEntries(cache: &cache) }
  }

  /// Materializes every key in the byte keyspace, used when installing a snapshot.
  public func allPairs() throws -> [KeyValue] {
    try scan(from: [], to: [UInt8.max])
  }

  /// Replaces the visible keyspace with one snapshot batch.
  public func replaceAll(with pairs: [KeyValue]) throws {
    try ensureOpen()
    var batch = WriteBatch()
    for pair in try allPairs() { batch.delete(pair.key) }
    for pair in pairs { batch.put(pair.value, for: pair.key) }
    try write(batch)
  }

  /// Commits every operation in `batch` under one new sequence number.
  ///
  /// Empty batches change nothing. A non-empty batch is synchronized to the
  /// active write-ahead log before it becomes visible in memory. A memory table
  /// that passes the configured byte threshold is frozen behind a new log;
  /// writing it out is ``maintain()``'s work, or this write's when no
  /// `maintain()` runs.
  ///
  /// - Parameter batch: Ordered puts and deletes to commit atomically.
  /// - Throws: `KwiftDBError.io` if the log, manifest, or table files cannot be
  ///   written; the log record is durable once its synchronization succeeds.
  public func write(_ batch: WriteBatch) throws {
    try ensureOpen()
    guard !batch.isEmpty else { return }
    let sequenced = SequencedBatch(
      sequence: manifest.lastSequence.incremented(),
      operations: batch.operations
    )
    try log.append(sequenced)
    memory.insert(sequenced)
    manifest.lastSequence = sequenced.sequence
    if memory.bytes > Tuning.memoryTableBytes {
      try freeze()
      if let maintenanceWake { maintenanceWake.yield() } else { try maintainHere() }
    }
  }

  /// Moves the memory table to the frozen list and starts a new log for the
  /// writes after it.
  private func freeze() throws {
    let number = manifest.takeFileNumber()
    let newLog = try LogWriter.create(at: Self.logURL(directory, number: number))
    try log.close()
    frozen.append(FrozenMemory(memory: memory, logs: memoryLogs))
    log = newLog
    memory = MemoryTable()
    memoryLogs = [number]
  }

  /// The first log any memory still needs, which the manifest records.
  var firstLiveLog: DatabaseUInt { frozen.first?.logs.first ?? memoryLogs.first ?? manifest.log }

  /// Removes a log whose writes are now in a table.
  func removeLog(_ number: DatabaseUInt) throws {
    try removeFile(Self.logURL(directory, number: number))
  }

  /// Closes the active log and all opened sorted tables.
  ///
  /// The store cannot be used after closing. Calling `close()` more than once
  /// is safe.
  ///
  /// - Throws: The first close failure reported by Foundation.
  public func close() throws {
    guard !closed else { return }
    closed = true
    var firstError: any Error?
    for table in tables {
      do { try table.close() }
      catch { if firstError == nil { firstError = error } }
    }
    do { try log.close() }
    catch { if firstError == nil { firstError = error } }
    if let firstError { throw firstError }
  }

  func ensureOpen() throws {
    if closed { throw KwiftDBError.io("store is closed") }
  }

  private func value(of version: Version) -> [UInt8]? {
    if case .put(let bytes) = version { return bytes }
    return nil
  }

  func removeFile(_ url: URL) throws {
    do {
      if FileManager.default.fileExists(atPath: url.path) {
        try FileManager.default.removeItem(at: url)
      }
    } catch { throw KwiftDBError.io(error.localizedDescription) }
  }

  private static func entryPrecedes(_ lhs: Entry, _ rhs: Entry) -> Bool {
    lhs.precedes(key: rhs.key, sequence: rhs.sequence)
  }

  private static func keyPrecedes(_ lhs: [UInt8], _ rhs: [UInt8]) -> Bool {
    lhs.lexicographicallyPrecedes(rhs)
  }

  static func newestFirst(_ tables: [SortedTable]) -> [SortedTable] {
    tables.sorted {
      if $0.record.level != $1.record.level { return $0.record.level < $1.record.level }
      return $1.record.number < $0.record.number
    }
  }

  static func sortedRecords(_ records: [TableRecord]) -> [TableRecord] {
    records.sorted {
      if $0.level != $1.level { return $0.level < $1.level }
      return $1.number < $0.number
    }
  }

  static func logURL(_ directory: URL, number: DatabaseUInt) -> URL {
    directory.appendingPathComponent("\(number).log", isDirectory: false)
  }

  static func tableURL(_ directory: URL, number: DatabaseUInt) -> URL {
    directory.appendingPathComponent("\(number).table", isDirectory: false)
  }

  /// The logs numbered from `first` on, oldest first: the manifest's log and
  /// the ones frozen memory tables started after it.
  static func liveLogs(in directory: URL, from first: DatabaseUInt) throws -> [DatabaseUInt] {
    let children: [URL]
    do { children = try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil) }
    catch { throw KwiftDBError.io(error.localizedDescription) }
    return children.compactMap { child -> DatabaseUInt? in
      let parts = child.lastPathComponent.split(separator: ".", omittingEmptySubsequences: false)
      guard parts.count == 2, parts[1] == "log", let number = try? DatabaseUInt(String(parts[0])),
            !(number < first) else { return nil }
      return number
    }.sorted { $0 < $1 }
  }

  /// Advances past orphaned allocated numbers and removes unreferenced files.
  private static func reconcileFiles(in directory: URL, manifest: inout Manifest) throws {
    let manager = FileManager.default
    let children: [URL]
    do { children = try manager.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil) }
    catch { throw KwiftDBError.io(error.localizedDescription) }

    var highestAllocated: DatabaseUInt?
    for child in children {
      let parts = child.lastPathComponent.split(separator: ".", omittingEmptySubsequences: false)
      guard parts.count == 2, parts[1] == "log" || parts[1] == "table",
            let number = try? DatabaseUInt(String(parts[0])) else { continue }
      if highestAllocated == nil || highestAllocated! < number { highestAllocated = number }
    }
    if let highestAllocated, manifest.nextFile <= highestAllocated {
      manifest.nextFile = highestAllocated.incremented()
      try ManifestFile.store(manifest, in: directory)
    }

    var retained = Set(manifest.tables.map { "\($0.number).table" })
    for number in try liveLogs(in: directory, from: manifest.log) { retained.insert("\(number).log") }
    for child in children {
      let name = child.lastPathComponent
      if name.hasSuffix(".log") || name.hasSuffix(".table") {
        if !retained.contains(name) {
          do { try manager.removeItem(at: child) }
          catch { throw KwiftDBError.io(error.localizedDescription) }
        }
      } else if name == ManifestFile.next {
        try? manager.removeItem(at: child)
      }
    }
  }
}
