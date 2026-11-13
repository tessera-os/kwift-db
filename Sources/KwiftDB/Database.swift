import Foundation

/// A database session that can read keys, scan ordered ranges, and commit
/// batches. `Database` accesses the durable store directly; a `RecordedRun`
/// implements the same surface while retaining its reads and writes locally.
public protocol DatabaseSession: Sendable {
  /// Reads the current value for an already encoded storage key.
  func readKey(_ key: [UInt8]) async throws -> [UInt8]?
  /// Reads visible pairs from the half-open byte range `[start, end)`.
  func scanKeys(from start: [UInt8], to end: [UInt8]) async throws -> [KeyValue]
  /// Commits a batch atomically.
  func writeBatch(_ batch: WriteBatch) async throws
}

/// A durable KwiftDB database with document and table faces.
///
/// A `Database` owns one `Store` actor for its directory. It serializes commits
/// through that actor and offers `runRecorded` for functions that need an
/// isolated read/write view. Close the database when its owning service stops;
/// a closed instance cannot be reopened, but the same directory can be opened
/// by a new instance after close completes.
public actor Database: DatabaseSession {
  private let store: Store

  /// Opens or creates a database directory and recovers its committed state.
  ///
  /// - Parameter directory: A directory dedicated to this database instance.
  /// - Throws: `KwiftDBError` when recovery or file creation fails.
  public init(directory: URL) throws {
    store = try Store(directory: directory)
  }

  /// Reads the bytes stored for an encoded storage key.
  public func readKey(_ key: [UInt8]) async throws -> [UInt8]? {
    try await store.get(key)
  }

  /// Scans the encoded-key range `[start, end)` in byte order.
  public func scanKeys(from start: [UInt8], to end: [UInt8]) async throws -> [KeyValue] {
    try await store.scan(from: start, to: end)
  }

  /// Commits the supplied operations as one durable sequence.
  public func writeBatch(_ batch: WriteBatch) async throws {
    try await store.write(batch)
  }

  /// Returns the sequence number of the latest committed storage batch.
  public func version() async throws -> Number { try await store.version() }

  /// Reads the full current keyspace in order for snapshot creation.
  public func allPairs() async throws -> [KeyValue] { try await store.allPairs() }

  /// Replaces the visible keyspace with a snapshot as one write batch.
  public func replaceAll(with pairs: [KeyValue]) async throws {
    try await store.replaceAll(with: pairs)
  }

  /// Creates a document collection handle using this database as its session.
  ///
  /// Collections are logical namespaces. Their keys share the document prefix
  /// and remain disjoint from catalog, index, and synchronization records.
  public func collection(named name: String) -> DocumentCollection {
    DocumentCollection(session: self, name: name)
  }

  /// Creates the catalog and query API for this database.
  public func tables() -> TableCatalog { TableCatalog(session: self) }

  /// Creates the durable stream API for this database.
  public func streams() -> DurableStreams { DurableStreams(session: self) }

  /// Runs a database operation against a repeatable, locally buffered view.
  ///
  /// Reads observe writes already held by the run. Each point read and range
  /// scan is recorded as a key range. A successful body commits all held writes
  /// as one batch; a throwing body discards them and returns its error text in
  /// the result while preserving the ranges for synchronization callers.
  /// Set `commitWrites` to `false` to capture a mutation for a higher-level
  /// transaction, such as sync's write-plus-cursor batch. Set `readOnly` to
  /// reject every attempted mutation, as required for query functions. Storage
  /// failures while committing are thrown from this method.
  ///
  /// - Parameter operation: An async operation that uses only the supplied
  ///   `RecordedRun` for database access.
  /// - Returns: The operation result, ranges read, and writes committed (or
  ///   discarded when the operation failed).
  public func runRecorded<T: Sendable>(
    commitWrites: Bool = true,
    readOnly: Bool = false,
    isolateCatalog: Bool = false,
    _ operation: @Sendable (RecordedRun) async throws -> T
  ) async throws -> RecordedRunResult<T> {
    let run = RecordedRun(store: store, readOnly: readOnly, isolateCatalog: isolateCatalog)
    let outcome: Result<T, any Error>
    do {
      outcome = .success(try await operation(run))
    } catch {
      outcome = .failure(error)
    }

    switch outcome {
    case .success(let value):
      let capture = await run.finish()
      if commitWrites { try await store.write(capture.batch) }
      return RecordedRunResult(
        outcome: .success(value),
        ranges: capture.ranges,
        writes: capture.batch.operations
      )
    case .failure(let error):
      let capture = await run.finish()
      return RecordedRunResult(
        outcome: .failure(error),
        ranges: capture.ranges,
        writes: []
      )
    }
  }

  /// Keeps the store in shape in the background until the calling task is
  /// cancelled (``Store/maintain()``). Without it, writes that fill the
  /// memory table do the flush and compaction work themselves.
  public nonisolated func maintain() async throws { try await store.maintain() }

  /// Closes the database's log and table handles.
  public func close() async throws { try await store.close() }

  /// Reads all stored data once to check it (``Store/verify()``).
  public func verify() async throws { try await store.verify() }
}

/// One half-open byte range observed during a recorded database run.
public struct KeyRange: Sendable, Equatable, Hashable {
  /// Inclusive first key in the range.
  public var start: [UInt8]
  /// Exclusive upper bound.
  public var end: [UInt8]

  /// Creates a range `[start, end)`.
  public init(start: [UInt8], end: [UInt8]) {
    self.start = start
    self.end = end
  }
}

/// The outcome and database effects captured by one recorded run.
public struct RecordedRunResult<Value: Sendable>: Sendable {
  /// A successful return value or the error the operation threw.
  public enum Outcome: Sendable {
    /// The operation returned normally.
    case success(Value)
    /// The operation threw; held writes were discarded.
    case failure(any Error)
  }

  /// The operation's result.
  public let outcome: Outcome
  /// Ordered ranges read by the operation, including reads before a failure.
  public let ranges: [KeyRange]
  /// Mutations committed for success, or an empty array after failure.
  public let writes: [WriteOperation]
}

extension RecordedRunResult.Outcome {
  /// The failure as clients read it: a `LocalizedError`'s own description,
  /// otherwise the error's description. Nil on success.
  public var failureText: String? {
    guard case .failure(let error) = self else { return nil }
    return (error as? any LocalizedError)?.errorDescription ?? String(describing: error)
  }
}

/// A rollback point for writes held by a recorded run.
///
/// Read ranges remain recorded when writes are rolled back, which is required
/// for subscriptions whose function fails after reading some keys.
public struct RecordedRunMark: Sendable {
  fileprivate let runID: UUID
  fileprivate let operations: [WriteOperation]
}

/// A recorded read/write view layered over a durable store.
///
/// Instances are created by `Database.runRecorded`. The actor keeps point and
/// range reads consistent with its held writes, coalesces repeated writes to a
/// key, and can be finished only once by the owning database.
public actor RecordedRun: DatabaseSession {
  private let store: Store
  private var readOnly: Bool
  private let isolateCatalog: Bool
  private let runID = UUID()
  private var held = WriteBatch()
  private var ranges: [KeyRange] = []
  private var finished = false

  internal init(store: Store, readOnly: Bool = false, isolateCatalog: Bool = false) {
    self.store = store
    self.readOnly = readOnly
    self.isolateCatalog = isolateCatalog
  }

  /// Reads a key and records the single-key range that contains it.
  public func readKey(_ key: [UInt8]) async throws -> [UInt8]? {
    try ensureActive()
    let storageKey = isolateCatalog ? CatalogKeySpace.remoteKey(key) : key
    let range = KeyRange(start: key, end: key + [0])
    ranges.append(isolateCatalog ? CatalogKeySpace.remoteRange(range) : range)
    if let operation = held.operation(for: storageKey) {
      if case .put(_, let value) = operation { return value }
      return nil
    }
    return try await store.get(storageKey)
  }

  /// Scans a range and overlays all held puts and deletes on the store result.
  public func scanKeys(from start: [UInt8], to end: [UInt8]) async throws -> [KeyValue] {
    try ensureActive()
    guard start.lexicographicallyPrecedes(end) else { return [] }
    let range = KeyRange(start: start, end: end)
    ranges.append(isolateCatalog ? CatalogKeySpace.remoteRange(range) : range)
    let storageRange = isolateCatalog ? CatalogKeySpace.remoteRange(range) : range
    var values = Dictionary(
      uniqueKeysWithValues: try await store.scan(from: storageRange.start, to: storageRange.end).map { (Data($0.key), $0.value) }
    )
    for operation in held.operations where operation.key.lexicographicallyPrecedes(storageRange.end)
      && !operation.key.lexicographicallyPrecedes(storageRange.start) {
      switch operation {
      case .put(let key, let value): values[Data(key)] = value
      case .delete(let key): values.removeValue(forKey: Data(key))
      }
    }
    return values.map {
      let key = Array($0.key)
      return KeyValue(key: isolateCatalog ? CatalogKeySpace.localKey(key) : key, value: $0.value)
    }
      .sorted { $0.key.lexicographicallyPrecedes($1.key) }
  }

  /// Reads the beginning of a range while recording only the keys needed to
  /// keep that prefix current. A following insertion or deletion invalidates
  /// the page, but rows after the first `count` keys are not replicated.
  ///
  /// The storage engine currently materializes the range on the server; the
  /// bounded dependency here prevents unrequested rows from reaching replicas.
  public func scanPrefix(from start: [UInt8], to end: [UInt8], count: Int) async throws -> [KeyValue] {
    try ensureActive()
    guard count > 0, start.lexicographicallyPrecedes(end) else { return [] }
    let range = KeyRange(start: start, end: end)
    let storageRange = isolateCatalog ? CatalogKeySpace.remoteRange(range) : range
    var values = Dictionary(
      uniqueKeysWithValues: try await store.scan(from: storageRange.start, to: storageRange.end).map { (Data($0.key), $0.value) }
    )
    for operation in held.operations where operation.key.lexicographicallyPrecedes(storageRange.end)
      && !operation.key.lexicographicallyPrecedes(storageRange.start) {
      switch operation {
      case .put(let key, let value): values[Data(key)] = value
      case .delete(let key): values.removeValue(forKey: Data(key))
      }
    }
    let ordered = values.map { KeyValue(key: Array($0.key), value: $0.value) }
      .sorted { $0.key.lexicographicallyPrecedes($1.key) }
    let page = Array(ordered.prefix(count))
    let scannedEnd = ordered.count >= count ? page[page.count - 1].key + [0] : storageRange.end
    ranges.append(KeyRange(start: storageRange.start, end: scannedEnd))
    return page.map { KeyValue(
      key: isolateCatalog ? CatalogKeySpace.localKey($0.key) : $0.key,
      value: $0.value
    ) }
  }

  /// Holds a put for this run; a later put or delete replaces it in place.
  public func put(_ value: [UInt8], for key: [UInt8]) throws {
    try ensureActive()
    try ensureWritable()
    held.put(value, for: isolateCatalog ? CatalogKeySpace.remoteKey(key) : key)
  }

  /// Holds a delete for this run; a later put replaces it in place.
  public func delete(_ key: [UInt8]) throws {
    try ensureActive()
    try ensureWritable()
    held.delete(isolateCatalog ? CatalogKeySpace.remoteKey(key) : key)
  }

  /// Adds every operation from a batch to the run's local write set.
  public func writeBatch(_ batch: WriteBatch) async throws {
    try ensureActive()
    try ensureWritable()
    for operation in batch.operations {
      switch operation {
      case .put(let key, let value): held.put(value, for: isolateCatalog ? CatalogKeySpace.remoteKey(key) : key)
      case .delete(let key): held.delete(isolateCatalog ? CatalogKeySpace.remoteKey(key) : key)
      }
    }
  }

  /// How many reads the run has recorded so far; ``ranges(since:)`` takes
  /// the ones after this point.
  public func readCount() -> Int { ranges.count }

  /// The ranges read since `readCount()` answered `start`.
  public func ranges(since start: Int) -> [KeyRange] { Array(ranges[min(start, ranges.count)...]) }

  /// The operations held so far, in order.
  public func heldOperations() -> [WriteOperation] { held.operations }

  /// Captures the current write set so failed speculative work can be undone.
  public func mark() throws -> RecordedRunMark {
    try ensureActive()
    return RecordedRunMark(runID: runID, operations: held.operations)
  }

  /// Restores the held writes from a mark while keeping all read ranges.
  public func rollback(to mark: RecordedRunMark) throws {
    try ensureActive()
    guard mark.runID == runID else { throw KwiftDBError.table("recorded-run mark belongs to another run") }
    held = WriteBatch()
    for operation in mark.operations {
      switch operation {
      case .put(let key, let value): held.put(value, for: key)
      case .delete(let key): held.delete(key)
      }
    }
  }

  /// Prevents any later write in this run while preserving its held changes.
  ///
  /// Replica views use this after applying pending mutations so subscribed
  /// query functions can read the projected state but cannot alter it.
  public func restrictWrites() throws {
    try ensureActive()
    readOnly = true
  }

  /// Creates table operations over this run's buffered view.
  public func tables() -> TableCatalog { TableCatalog(session: self) }

  /// Opens a document collection over this run's buffered view.
  ///
  /// Reads are recorded as the run's ranges, so a query that reads a document
  /// is re-run when that document changes; writes are held until the run ends
  /// and apply only when the function answers successfully.
  public func collection(named name: String) -> DocumentCollection {
    DocumentCollection(session: self, name: name)
  }

  /// Creates stream operations over this run's buffered view.
  public func streams() -> DurableStreams { DurableStreams(session: self) }

  fileprivate func finish() -> (ranges: [KeyRange], batch: WriteBatch) {
    finished = true
    return (ranges, held)
  }

  private func ensureActive() throws {
    if finished { throw KwiftDBError.io("recorded run is already finished") }
  }

  private func ensureWritable() throws {
    if readOnly { throw KwiftDBError.table("this recorded run is read-only") }
  }
}
