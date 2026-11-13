import Foundation

extension Store {
  /// Writes frozen memory tables out and compacts crowded levels, off the
  /// store's actor, until the calling task is cancelled.
  ///
  /// While it runs, a write never does this upkeep itself, so no read or
  /// write waits for it. Run one at a time per store, for as long as the store
  /// is open; a job in progress when the task is cancelled still finishes.
  public func maintain() async throws {
    guard maintenanceWake == nil else { return }
    let (wakes, wake) = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
    maintenanceWake = wake
    defer { maintenanceWake = nil }
    wake.yield()
    for await _ in wakes {
      while let job = try nextJob() {
        let written = try await job.runConcurrently()
        try install(job, written: written)
      }
    }
  }

  /// Runs every pending job on the actor, for a store no `maintain()` serves.
  func maintainHere() throws {
    while let job = try nextJob() { try install(job, written: try job.run()) }
  }

  /// The next upkeep: the oldest frozen memory table, else the most crowded
  /// level's compaction, else nil.
  func nextJob() throws -> MaintenanceJob? {
    try ensureOpen()
    if let oldest = frozen.first {
      return MaintenanceJob(work: .flush(oldest.memory), level: 0, numbers: [manifest.takeFileNumber()], directory: directory)
    }
    guard let level = crowdedLevel() else { return nil }
    let inputs = compactionInputs(level: level)
    guard !inputs.isEmpty else { return nil }
    let target = level + 1
    let selected = Set(inputs.map(\.record.number))
    let smallest = inputs.map(\.record.smallest).min { $0.lexicographicallyPrecedes($1) }!
    let largest = inputs.map(\.record.largest).max { $0.lexicographicallyPrecedes($1) }!
    // A deletion still hides something only while a deeper table holds its key.
    let deeper = tables.contains {
      $0.record.level > target && !selected.contains($0.record.number) && $0.record.overlaps(smallest, largest)
    }
    let bytes = inputs.reduce(UInt64(0)) { $0 + $1.size }
    let count = Int(bytes / UInt64(Tuning.tableBytes)) + 2
    compactionPointers[level] = largest
    return MaintenanceJob(
      work: .compact(inputs: inputs.map(\.record), dropsDeletions: !deeper),
      level: target, numbers: (0..<count).map { _ in manifest.takeFileNumber() }, directory: directory)
  }

  /// Puts a finished job's tables in the manifest and removes what they replace.
  func install(_ job: MaintenanceJob, written: [WrittenTable]) throws {
    try ensureOpen()
    var opened: [SortedTable] = []
    do {
      for table in written {
        opened.append(try SortedTable.open(record: table.record, at: Self.tableURL(directory, number: table.record.number)))
      }
    } catch {
      for table in opened { try? table.close() }
      throw error
    }
    var proposed = manifest
    var retiredTables: [SortedTable] = []
    var retiredLogs: [DatabaseUInt] = []
    switch job.work {
    case .flush:
      retiredLogs = frozen.removeFirst().logs
    case .compact(let inputs, _):
      let numbers = Set(inputs.map(\.number))
      retiredTables = tables.filter { numbers.contains($0.record.number) }
      proposed.tables.removeAll { numbers.contains($0.number) }
    }
    proposed.tables = Self.sortedRecords(proposed.tables + written.map(\.record))
    proposed.log = firstLiveLog
    try ManifestFile.store(proposed, in: directory)
    manifest = proposed
    let retiredNumbers = Set(retiredTables.map(\.record.number))
    tables = Self.newestFirst(tables.filter { !retiredNumbers.contains($0.record.number) } + opened)
    for table in retiredTables {
      try table.close()
      try removeFile(Self.tableURL(directory, number: table.record.number))
    }
    for number in retiredLogs { try removeLog(number) }
    // A compaction can leave the next level crowded.
    maintenanceWake?.yield()
  }

  private func crowdedLevel() -> Int? {
    if tables.filter({ $0.record.level == 0 }).count >= Tuning.levelZeroTables { return 0 }
    let deepest = tables.map(\.record.level).max() ?? 0
    guard deepest >= 1 else { return nil }
    var limit = Tuning.levelBytes
    for level in 1...deepest {
      if level > 1 { limit = limit > UInt64.max / 10 ? UInt64.max : limit * 10 }
      let bytes = tables.filter { $0.record.level == level }.reduce(UInt64(0)) { $0 &+ $1.size }
      if bytes > limit { return level }
    }
    return nil
  }

  /// Level 0's tables overlap, so all of them go down together; a deeper
  /// level sends one table, the next after the last one sent. Either way the
  /// tables of the next level that overlap them join the merge, and no other.
  private func compactionInputs(level: Int) -> [SortedTable] {
    let sources: [SortedTable]
    if level == 0 {
      sources = tables.filter { $0.record.level == 0 }
    } else {
      let candidates = tables.filter { $0.record.level == level }
        .sorted { $0.record.smallest.lexicographicallyPrecedes($1.record.smallest) }
      let after = compactionPointers[level]
      guard let next = candidates.first(where: { after.map($0.record.smallest.lexicographicallyPrecedes) == false && $0.record.smallest != after })
        ?? candidates.first else { return [] }
      sources = [next]
    }
    guard !sources.isEmpty else { return [] }
    let smallest = sources.map(\.record.smallest).min { $0.lexicographicallyPrecedes($1) }!
    let largest = sources.map(\.record.largest).max { $0.lexicographicallyPrecedes($1) }!
    let overlapping = tables.filter { $0.record.level == level + 1 && $0.record.overlaps(smallest, largest) }
    return sources + overlapping
  }
}
