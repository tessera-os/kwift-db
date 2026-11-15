import Foundation
import Logging

/// An event produced by a replica as sync state changes.
public enum ReplicaEvent: Sendable {
  /// Authentication completed and the server supplied this identity.
  case welcomed(Value)
  /// The server refused the current session.
  case refused(String)
  /// A subscribed query now returns a different value over the local view.
  case changed(query: Number, value: Value)
  /// The server answered or skipped a pending mutation.
  case done(number: Number, outcome: SyncMutationOutcome)
  /// A pending mutation now has another number, after the server refused it
  /// as out of order; its outcome arrives as `done` under the new number.
  case renumbered(from: Number, to: Number)
  /// The server has sent a subscribed query's complete first result, so its
  /// value now reflects the server rather than only what the replica held.
  case ready(query: Number)
}

/// Equality terms that identify the rows owned by one local mirror partition.
public struct TableMirrorScope: Sendable {
  /// Required equality comparisons, such as origin and conversation id.
  public var equalities: [Comparison]

  /// Creates a scope and rejects non-equality terms.
  public init(equalities: [Comparison]) throws {
    guard equalities.allSatisfy({ $0.relation == .equal }) else {
      throw KwiftDBError.table("table mirror scope accepts equality comparisons only")
    }
    self.equalities = equalities
  }
}

/// A durable client replica that applies server transitions to its own database.
///
/// The replica stores its server-assigned client id, identity, version, held
/// ranges, next mutation number, and pending mutation arguments under the
/// reserved sync keyspace. Subscriptions last for this open instance and are
/// sent again on every `connect`, while pending mutations survive reopening.
public actor Replica {
  private let database: Database
  private let functions: Functions
  private var loaded = false
  private var client = Number.zero
  private var identity: Value = .null
  private var version = Number.zero
  private var nextMutation = DatabaseUInt(1)
  private var nextQuery = DatabaseUInt(1)
  private var pending: [DatabaseUInt: PendingMutation] = [:]
  private var subscriptions: [Number: ReplicaSubscription] = [:]
  private var held: [KeyRange] = []
  private var stale: [KeyRange] = []
  private var ready: Set<Number> = []
  private var connected = false
  /// The client's last applied mutation number as this session's latest
  /// transition reported it; nil until the session's first transition.
  private var sessionApplied: DatabaseUInt?
  private var outgoing: [SyncMessage] = []
  private var events: [ReplicaEvent] = []
  private var lastResults: [Number: Value] = [:]
  /// The storage ranges each subscribed query read in the last refresh: a
  /// query is run again only when what changed overlaps them.
  private var queryRanges: [Number: [KeyRange]] = [:]
  /// Why each query failed over the local view in the last refresh, so a
  /// failure is logged when it starts or changes, not on every refresh.
  private var queryFailures: [Number: String] = [:]
  /// The keys pending mutations wrote in the last refresh, as point ranges.
  private var pendingKeys: [KeyRange] = []
  private var tableCatalogIsolation = false
  private let retention: ReplicaRetention
  /// The directory ``open(directory:functions:retention:)`` claimed, released on ``close()``.
  private var claimedDirectory: URL?
  /// Whether an operation is running. Actors interleave at every `await`, and
  /// each operation reads state, suspends on storage, then writes state back,
  /// so two overlapping ones would overwrite each other's ready flags, held
  /// ranges and query results. Operations run one at a time instead.
  private var busy = false
  private var turnWaiters: [CheckedContinuation<Void, Never>] = []
  /// Where the running operation's time went, for the slow-operation log.
  private var breakdown: [String] = []
  /// How many unsubscribed queries stay subscribed, newest kept.
  private var lingerLimit = 0
  /// The query names that linger; nil for every one.
  private var lingerNames: Set<String>?
  /// Queries no reader holds any more but still subscribed, least recently
  /// let go first. Subscribing to one again takes it back with its value and
  /// readiness as they are, so a page opened again shows at once.
  private var lingering: [Number] = []

  /// Opens or creates a replica directory using the shared function registry.
  ///
  /// - Parameter retention: Whether rows the server releases are removed (the
  ///   spec's `.exact`, the default) or kept as an offline cache.
  public init(directory: URL, functions: Functions, retention: ReplicaRetention = .exact) throws {
    database = try Database(directory: directory)
    self.functions = functions
    self.retention = retention
    claimedDirectory = nil
  }

  /// Opens a replica directory that no other replica in this process has
  /// open, waiting for one that has it to ``close()``.
  ///
  /// Two stores open on one directory corrupt it, so an app that may open an
  /// account's replica again before the previous one is gone opens it here
  /// and closes it when done. A replica already open in another process makes
  /// this call throw instead of waiting.
  ///
  /// - Throws: When another process has the directory open or its storage
  ///   cannot be opened.
  public static func open(
    directory: URL, functions: Functions, retention: ReplicaRetention = .exact
  ) async throws -> Replica {
    await DirectoryClaims.shared.claim(directory)
    do {
      return try Replica(claimed: directory, functions: functions, retention: retention)
    } catch {
      await DirectoryClaims.shared.release(directory)
      throw error
    }
  }

  private init(claimed directory: URL, functions: Functions, retention: ReplicaRetention) throws {
    database = try Database(directory: directory)
    self.functions = functions
    self.retention = retention
    claimedDirectory = directory
  }

  /// Registers a query for this replica's lifetime and returns its query id.
  public func subscribe(name: String, arguments: Value) async throws -> Number {
    let arrived = ContinuousClock.now
    await beginTurn()
    let started = ContinuousClock.now
    defer {
      endTurn()
      Self.logIfSlow("subscribe \(name)", waited: started - arrived, worked: ContinuousClock.now - started)
    }
    try await ensureLoaded()
    if let kept = lingering.firstIndex(where: {
      subscriptions[$0]?.name == name && subscriptions[$0]?.arguments == arguments
    }) {
      // Still subscribed and kept live by the server: its value is current.
      // `value(of:)` and `isReady(_:)` give it at once.
      return lingering.remove(at: kept)
    }
    let query = try Number(nextQuery.description)
    nextQuery = nextQuery.incremented()
    subscriptions[query] = ReplicaSubscription(name: name, arguments: arguments)
    ready.remove(query)
    if connected { outgoing.append(.subscribe(query: query, name: name, arguments: arguments)) }
    await refreshView(changed: [])
    return query
  }

  /// Keeps up to `limit` queries subscribed after ``unsubscribe(_:)``, the
  /// most recently let go first, so subscribing to one again finds its value
  /// and readiness at once. Zero, the default, ends every subscription when
  /// it is let go, as the spec has it.
  ///
  /// - Parameter names: The queries that linger; nil for all. Queries
  ///   subscribed once each, such as a search per keystroke, belong out of
  ///   it, or they push out the ones worth keeping.
  public func keepLingering(_ limit: Int, names: Set<String>? = nil) async throws {
    await beginTurn()
    defer { endTurn() }
    try await ensureLoaded()
    lingerLimit = max(limit, 0)
    lingerNames = names
    dropLingering()
  }

  /// Removes a query subscription and informs an open server session; with
  /// ``keepLingering(_:)`` the query stays subscribed until newer ones push
  /// it out.
  public func unsubscribe(_ query: Number) async throws {
    await beginTurn()
    defer { endTurn() }
    try await ensureLoaded()
    guard let subscription = subscriptions[query], !lingering.contains(query) else { return }
    guard lingerNames?.contains(subscription.name) ?? true else { return end(query) }
    lingering.append(query)
    dropLingering()
  }

  /// Ends the oldest lingering subscriptions past the limit.
  private func dropLingering() {
    while lingering.count > lingerLimit {
      end(lingering.removeFirst())
    }
  }

  private func end(_ query: Number) {
    subscriptions.removeValue(forKey: query)
    lastResults.removeValue(forKey: query)
    queryRanges.removeValue(forKey: query)
    ready.remove(query)
    if connected { outgoing.append(.unsubscribe(query: query)) }
  }

  /// Adds a durable pending mutation and applies it speculatively to the local view.
  ///
  /// - Throws: `FunctionFailure.noSuchFunction` for a mutation registered with
  ///   `.server` access, which clients cannot call.
  public func mutate(name: String, arguments: Value) async throws -> Number {
    let arrived = ContinuousClock.now
    await beginTurn()
    let started = ContinuousClock.now
    defer {
      endTurn()
      Self.logIfSlow("mutate \(name)", waited: started - arrived, worked: ContinuousClock.now - started)
    }
    // Server-only mutations do not exist for clients, as on the server.
    if functions.mutation(named: name)?.access == .server {
      throw FunctionFailure.noSuchFunction(name)
    }
    try await ensureLoaded()
    let number = nextMutation
    nextMutation = nextMutation.incremented()
    let publicNumber = try Number(number.description)
    pending[number] = PendingMutation(name: name, arguments: arguments)
    let pendingKey = try Self.pendingKey(publicNumber)
    let nextKey = try Self.syncKey(.text("next-mutation"))
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.document(try Document([
      DocumentField("name", .text(name)), DocumentField("arguments", arguments),
    ])))), for: pendingKey)
    batch.put(Array(try ValueCodec.encode(.number(try Number(nextMutation.description)))), for: nextKey)
    try await database.writeBatch(batch)
    if connected { outgoing.append(.mutate(number: publicNumber, name: name, arguments: arguments)) }
    if functions.mutation(named: name)?.speculative != false { await refreshView(changed: []) }
    return publicNumber
  }

  /// Starts a session by sending hello, every subscription, and pending mutations in order.
  public func connect(credentials: Value) async throws -> [SyncMessage] {
    let arrived = ContinuousClock.now
    await beginTurn()
    let started = ContinuousClock.now
    defer {
      endTurn()
      Self.logIfSlow("connect", waited: started - arrived, worked: ContinuousClock.now - started)
    }
    try await ensureLoaded()
    stale = Self.normalized(stale + held)
    held = []
    ready.removeAll(keepingCapacity: true)
    connected = true
    sessionApplied = nil
    var messages: [SyncMessage] = [.hello(client: client, credentials: credentials)]
    for (query, subscription) in subscriptions.sorted(by: { $0.key < $1.key }) {
      messages.append(.subscribe(query: query, name: subscription.name, arguments: subscription.arguments))
    }
    for (number, mutation) in pending.sorted(by: { $0.key < $1.key }) {
      messages.append(.mutate(number: try Number(number.description), name: mutation.name, arguments: mutation.arguments))
    }
    return messages
  }

  /// Ends local session state while retaining persisted held ranges as stale data.
  public func disconnect() async throws {
    await beginTurn()
    defer { endTurn() }
    try await ensureLoaded()
    stale = Self.normalized(stale + held)
    held = []
    ready.removeAll(keepingCapacity: true)
    connected = false
    outgoing.removeAll(keepingCapacity: true)
  }

  /// Applies a server message and returns newly observable replica events.
  public func receive(_ message: SyncMessage) async throws -> [ReplicaEvent] {
    let arrived = ContinuousClock.now
    await beginTurn()
    let started = ContinuousClock.now
    breakdown.removeAll(keepingCapacity: true)
    defer {
      endTurn()
      Self.logIfSlow(Self.describe(message), waited: started - arrived, worked: ContinuousClock.now - started, breakdown: breakdown)
    }
    try await ensureLoaded()
    switch message {
    case .welcome(let serverClient, let serverIdentity):
      if client.isZero { client = serverClient }
      identity = serverIdentity
      var batch = WriteBatch()
      let clientKey = try Self.syncKey(.text("client"))
      let identityKey = try Self.syncKey(.text("identity"))
      batch.put(Array(try ValueCodec.encode(.number(client))), for: clientKey)
      batch.put(Array(try ValueCodec.encode(identity)), for: identityKey)
      try await database.writeBatch(batch)
      events.append(.welcomed(identity))
    case .refused(let reason):
      connected = false
      if reason == SyncServer.outOfOrder, let applied = sessionApplied {
        try await renumberPending(after: applied)
      }
      events.append(.refused(reason))
    case .transition(let serverVersion, let applied, let changes, let newlyReady, let results):
      let changed = try await applyTransition(version: serverVersion, applied: applied, changes: changes, newlyReady: newlyReady)
      for (number, outcome) in results {
        events.append(.done(number: number, outcome: outcome))
      }
      let refreshing = ContinuousClock.now
      await refreshView(changed: changed)
      note("refresh", since: refreshing)
      for item in newlyReady where subscriptions[item.query] != nil {
        events.append(.ready(query: item.query))
      }
    case .hello, .subscribe, .unsubscribe, .mutate, .heartbeat:
      throw KwiftDBError.invalidEncoding("replica received a client-only sync message")
    }
    let result = events
    events.removeAll(keepingCapacity: true)
    return result
  }

  /// Takes locally queued client messages after subscribe, unsubscribe, or mutate.
  public func takeOutgoing() -> [SyncMessage] {
    defer { outgoing.removeAll(keepingCapacity: true) }
    return outgoing
  }

  /// Takes the events local calls produced: a new subscription's first value
  /// over the local view, and the speculative effect of `mutate` on every
  /// subscribed query. Server messages return their events from `receive`.
  public func takeEvents() -> [ReplicaEvent] {
    defer { events.removeAll(keepingCapacity: true) }
    return events
  }

  /// A subscribed query's current value over the local view, including
  /// pending mutations, or nil when it has none yet.
  public func value(of query: Number) -> Value? {
    lastResults[query]
  }

  /// Whether the server has sent `query`'s complete first result in this
  /// session (`ReplicaEvent.ready`).
  public func isReady(_ query: Number) -> Bool {
    ready.contains(query)
  }

  /// Keeps the replica's store in shape until the calling task is cancelled
  /// (``Database/maintain()``). It takes no turn: upkeep runs beside the
  /// replica's operations, never ahead of them.
  public nonisolated func maintain() async throws { try await database.maintain() }

  /// Reads the replica's whole store once, throwing
  /// `KwiftDBError.corruptStorage` when part of it cannot be read back.
  public func verifyStorage() async throws {
    await beginTurn()
    defer { endTurn() }
    try await database.verify()
  }

  /// Closes the durable replica database and, for a replica made by
  /// ``open(directory:functions:retention:)``, frees its directory.
  public func close() async throws {
    await beginTurn()
    defer { endTurn() }
    let directory = claimedDirectory
    claimedDirectory = nil
    var failure: (any Error)?
    do { try await database.close() } catch { failure = error }
    // Released even when closing failed: the store is unusable either way.
    if let directory { await DirectoryClaims.shared.release(directory) }
    if let failure { throw failure }
  }

  /// The durable client id learned from the server, or zero before first welcome.
  public func clientID() async throws -> Number {
    await beginTurn()
    defer { endTurn() }
    try await ensureLoaded()
    return client
  }

  /// The most recent server version applied to the replica.
  public func serverVersion() async throws -> Number {
    await beginTurn()
    defer { endTurn() }
    try await ensureLoaded()
    return version
  }

  /// Keeps the device's `c` catalog local while translating synced catalogs to
  /// the reserved `sync-remote-catalog` keyspace for recorded functions.
  ///
  /// Call this before connecting when the replica also owns device-local
  /// tables. Incoming catalog writes, holds, and releases are translated, and
  /// query/mutation functions in the speculative view resolve remote table ids
  /// without replacing the local catalog.
  public func isolateTableCatalog() async throws {
    await beginTurn()
    defer { endTurn() }
    try await ensureLoaded()
    tableCatalogIsolation = true
  }

  /// Returns the catalog for device-local tables in the replica database.
  public func tables() async throws -> TableCatalog {
    await beginTurn()
    defer { endTurn() }
    try await ensureLoaded()
    return await database.tables()
  }

  /// Reconciles a subscribed `List(Document)` into a local named table.
  ///
  /// The method validates and projects the complete source set before writing,
  /// removes fields computed only by the server, rejects duplicate primary
  /// keys and rows outside `scope`, then applies inserts, replacements, and
  /// deletions in one recorded run. Rows outside the scope stay untouched.
  public func materializeTableQuery(
    query: Number,
    tableName: String,
    scope: TableMirrorScope
  ) async throws {
    await beginTurn()
    defer { endTurn() }
    try await ensureLoaded()
    guard let source = lastResults[query], case .list(let sourceRows) = source else {
      throw KwiftDBError.table("subscribed query is not a ready List(Document)")
    }
    let localCatalog = await database.tables()
    guard let currentTable = try await localCatalog.table(named: tableName) else {
      throw KwiftDBError.table("local mirror table does not exist: \(tableName)")
    }
    let projected = try Self.projectMirrorRows(sourceRows, schema: currentTable.schema, scope: scope)
    let conditions = scope.equalities.map(Condition.compare)
    let database = self.database
    let reconciliation = try await database.runRecorded { run in
      let catalog = TableCatalog(session: run)
      guard let table = try await catalog.table(named: tableName), table.id == currentTable.id else {
        throw KwiftDBError.table("local mirror table changed during reconciliation")
      }
      let existing = try await catalog.query(Query(table: table, conditions: conditions))
      var byKey: [Data: Document] = [:]
      for row in existing { byKey[try Self.mirrorPrimaryKey(row, schema: table.schema)] = row }
      let sourceKeys = Set(projected.map { $0.key })
      for (key, row) in projected {
        if byKey[key] != row { try await catalog.replace(row, in: table) }
      }
      for (key, row) in byKey where !sourceKeys.contains(key) {
        try await catalog.delete(table, primaryKey: try Self.mirrorPrimaryValues(row, schema: table.schema))
      }
    }
    if case .failure(let error) = reconciliation.outcome {
      throw KwiftDBError.table("mirror reconciliation failed: \(error)")
    }
  }

  private static let logger = Logger(label: "kwiftdb.replica")

  /// Logs an operation that took 100 ms or more: how long it waited for the
  /// one before it, and how long its own work took.
  private static func logIfSlow(_ operation: String, waited: Duration, worked: Duration, breakdown: [String] = []) {
    guard waited + worked >= .milliseconds(100) else { return }
    logger.info("slow replica operation", metadata: [
      "operation": "\(operation)", "wait_ms": "\(waited.milliseconds)", "work_ms": "\(worked.milliseconds)",
      "breakdown": "\(breakdown.joined(separator: " "))",
    ])
  }

  /// Notes in the running operation's breakdown how long `phase` took.
  private func note(_ phase: String, since start: ContinuousClock.Instant) {
    breakdown.append("\(phase)=\((ContinuousClock.now - start).milliseconds)")
  }

  private static func describe(_ message: SyncMessage) -> String {
    switch message {
    case .transition(_, _, let changes, let ready, let results):
      "transition changes=\(changes.count) ready=\(ready.count) results=\(results.count)"
    case .welcome: "welcome"
    case .refused: "refused"
    default: "message"
    }
  }

  /// Waits until no other operation runs, then marks this one running.
  private func beginTurn() async {
    if !busy {
      busy = true
      return
    }
    await withCheckedContinuation { turnWaiters.append($0) }
  }

  /// Hands the replica to the longest waiting operation, or frees it.
  private func endTurn() {
    if turnWaiters.isEmpty {
      busy = false
    } else {
      turnWaiters.removeFirst().resume()
    }
  }

  private func ensureLoaded() async throws {
    guard !loaded else { return }
    if let value = try await readValue(Self.syncKey(.text("client"))), case .number(let number) = value { client = number }
    if let value = try await readValue(Self.syncKey(.text("identity"))) { identity = value }
    if let value = try await readValue(Self.syncKey(.text("version"))), case .number(let number) = value { version = number }
    if let value = try await readValue(Self.syncKey(.text("next-mutation"))), case .number(let number) = value {
      nextMutation = try Self.unsigned(number)
    }
    if let value = try await readValue(Self.syncKey(.text("held"))) { held = try Self.decodeRanges(value) }
    stale = held
    let prefix = try KeyEncoder.encodeTuple([.text("s"), .text("pending")])
    for pair in try await database.scanKeys(from: prefix, to: DocumentCollection.upperBound(after: prefix)) {
      let tuple = try KeyDecoder.decodeTuple(pair.key)
      guard tuple.count == 3, case .number(let number) = tuple[2],
            case .document(let document) = try ValueCodec.decode(Data(pair.value)),
            case .text(let name)? = document["name"], let arguments = document["arguments"] else {
        throw KwiftDBError.corruptStorage("malformed pending sync mutation")
      }
      pending[try Self.unsigned(number)] = PendingMutation(name: name, arguments: arguments)
    }
    loaded = true
  }

  private func applyTransition(
    version serverVersion: Number,
    applied: Number,
    changes: [SyncChange],
    newlyReady: [SyncReady]
  ) async throws -> [KeyRange] {
    var batch = WriteBatch()
    var changed: [KeyRange] = []
    // A transition's holds and releases never cover the same keys (the
    // server sends what it newly holds and what it let go), so they are
    // merged into the held ranges once, after the loop.
    var holds: [KeyRange] = []
    var releases: [KeyRange] = []
    var staleAdded: [KeyRange] = []
    var nextHeld = held
    var nextStale = stale
    var nextReady = ready
    var phase = ContinuousClock.now
    for change in changes {
      switch change {
      case .write(let key, let value):
        let storageKey = tableCatalogIsolation ? CatalogKeySpace.remoteKey(key) : key
        if let value { batch.put(value, for: storageKey) } else { batch.delete(storageKey) }
        changed.append(KeyRange(start: storageKey, end: storageKey + [0]))
      case .hold(let range, let pairs):
        // A hold restates the range's rows. Rows already stored as sent are
        // left alone and not reported as changed, so a resubscription after
        // a reconnect re-runs no query whose rows did not move.
        let storageRange = tableCatalogIsolation ? CatalogKeySpace.remoteRange(range) : range
        let scanning = ContinuousClock.now
        var stored: [[UInt8]: [UInt8]] = [:]
        for item in try await database.scanKeys(from: storageRange.start, to: storageRange.end) {
          stored[item.key] = item.value
        }
        note("hold-scan(stored=\(stored.count),sent=\(pairs.count))", since: scanning)
        for pair in pairs {
          let storageKey = tableCatalogIsolation ? CatalogKeySpace.remoteKey(pair.key) : pair.key
          if stored.removeValue(forKey: storageKey) == pair.value { continue }
          batch.put(pair.value, for: storageKey)
          changed.append(KeyRange(start: storageKey, end: storageKey + [0]))
        }
        for key in stored.keys {
          batch.delete(key)
          changed.append(KeyRange(start: key, end: key + [0]))
        }
        holds.append(storageRange)
      case .release(let range):
        let storageRange = tableCatalogIsolation ? CatalogKeySpace.remoteRange(range) : range
        switch retention {
        case .exact:
          for item in try await database.scanKeys(from: storageRange.start, to: storageRange.end) { batch.delete(item.key) }
        case .keepReleased:
          staleAdded.append(storageRange)
        }
        releases.append(storageRange)
        changed.append(storageRange)
      }
    }
    if !holds.isEmpty || !releases.isEmpty {
      nextHeld = Self.subtract(Self.normalized(nextHeld + holds), releases)
    }
    if !staleAdded.isEmpty { nextStale = Self.normalized(nextStale + staleAdded) }
    for item in newlyReady { nextReady.insert(item.query) }

    let allReady = subscriptions.keys.allSatisfy(nextReady.contains)
    if allReady {
      switch retention {
      case .exact:
        for range in Self.subtract(nextStale, nextHeld) {
          for item in try await database.scanKeys(from: range.start, to: range.end) { batch.delete(item.key) }
        }
        nextStale = []
      case .keepReleased:
        // Stale ranges stay as the cache; the held part is the server's now.
        nextStale = Self.subtract(nextStale, nextHeld)
      }
    }

    note("changes", since: phase)
    phase = ContinuousClock.now
    let versionKey = try Self.syncKey(.text("version"))
    let heldKey = try Self.syncKey(.text("held"))
    batch.put(Array(try ValueCodec.encode(.number(serverVersion))), for: versionKey)
    batch.put(Array(try ValueCodec.encode(.list(try Self.encodeRanges(nextHeld + nextStale)))), for: heldKey)
    note("held(\(nextHeld.count)+\(nextStale.count))", since: phase)
    phase = ContinuousClock.now
    let pendingPrefix = try KeyEncoder.encodeTuple([.text("s"), .text("pending")])
    let appliedNumber = try Self.unsigned(applied)
    sessionApplied = appliedNumber
    var completedMutations: [DatabaseUInt] = []
    for pair in try await database.scanKeys(from: pendingPrefix, to: DocumentCollection.upperBound(after: pendingPrefix)) {
      let tuple = try KeyDecoder.decodeTuple(pair.key)
      guard tuple.count == 3, case .number(let number) = tuple[2] else {
        throw KwiftDBError.corruptStorage("malformed pending mutation key")
      }
      let sequence = try Self.unsigned(number)
      if sequence <= appliedNumber {
        batch.delete(pair.key)
        completedMutations.append(sequence)
      }
    }
    note("pending", since: phase)
    phase = ContinuousClock.now
    try await database.writeBatch(batch)
    note("write", since: phase)
    version = serverVersion
    held = nextHeld
    stale = nextStale
    ready = nextReady
    for number in completedMutations { pending.removeValue(forKey: number) }
    return KeyRanges.normalized(changed)
  }

  /// Recomputes the subscribed queries over the local view: the stored rows
  /// with every pending mutation applied.
  ///
  /// - Parameter changed: The storage ranges whose rows changed since the
  ///   last refresh, or nil when anything may have. A query whose last reads
  ///   miss them, and miss what pending mutations write now or wrote last
  ///   time, keeps its last result without running.
  private func refreshView(changed: [KeyRange]? = nil) async {
    let old = lastResults
    let knownRanges = queryRanges
    let previousPending = pendingKeys
    let pendingSnapshot = pending.sorted(by: { $0.key < $1.key })
    let subscriptionSnapshot = subscriptions
    let caller = Caller(client: client, identity: identity)
    let registry = functions
    do {
      let result = try await database.runRecorded(commitWrites: false, isolateCatalog: tableCatalogIsolation) { run in
        for (_, mutation) in pendingSnapshot {
          guard let function = registry.mutation(named: mutation.name), function.speculative else { continue }
          let mark = try await run.mark()
          do { _ = try await function.run(run, caller, mutation.arguments) }
          catch { try await run.rollback(to: mark) }
        }
        let pendingWrites = KeyRanges.normalized(await run.heldOperations().map { KeyRange(start: $0.key, end: $0.key + [0]) })
        try await run.restrictWrites()
        let dirty = changed.map { KeyRanges.normalized($0 + pendingWrites + previousPending) }
        var values: [Number: Value] = [:]
        var ranges: [Number: [KeyRange]] = [:]
        var reran: [String] = []
        var failures: [Number: String] = [:]
        for (query, subscription) in subscriptionSnapshot {
          if let dirty, let known = knownRanges[query], let previous = old[query], !KeyRanges.overlap(known, dirty) {
            values[query] = previous
            ranges[query] = known
            continue
          }
          guard let function = registry.query(named: subscription.name) else { continue }
          reran.append(subscription.name)
          let start = await run.readCount()
          do { values[query] = try await function.run(run, caller, subscription.arguments) }
          catch { failures[query] = "\(subscription.name): \(error)" }
          ranges[query] = KeyRanges.normalized(await run.ranges(since: start))
        }
        return RefreshedView(
          values: values, ranges: ranges, pendingWrites: pendingWrites, reran: reran, failures: failures)
      }
      guard case .success(let view) = result.outcome else { return }
      breakdown.append("queries=\(subscriptionSnapshot.count) reran=\(view.reran.joined(separator: ","))")
      lastResults = view.values
      queryRanges = view.ranges
      pendingKeys = view.pendingWrites
      // A query that fails here keeps no local value, so its subscriber
      // waits on the server alone: say why once, or the wait has no
      // explanation.
      for (query, failure) in view.failures where queryFailures[query] != failure {
        Self.logger.warning("local query failed", metadata: ["failure": "\(failure)"])
      }
      queryFailures = view.failures
      for (query, value) in view.values where old[query] != value {
        events.append(.changed(query: query, value: value))
      }
    } catch {
      // A failed local projection has no committed effect; the next database
      // event or query registration retries it with a fresh recorded run.
    }
  }

  private func readValue(_ key: [UInt8]) async throws -> Value? {
    guard let data = try await database.readKey(key) else { return nil }
    return try ValueCodec.decode(Data(data))
  }

  private static func projectMirrorRows(_ source: [Value], schema: TableSchema, scope: TableMirrorScope) throws -> [(key: Data, row: Document)] {
    try schema.validate()
    for term in scope.equalities {
      guard let column = schema.columnsByName[term.column], column.type.isOrderable,
            term.value != .null, column.type.accepts(term.value) else {
        throw KwiftDBError.table("mirror scope does not match local column \(term.column)")
      }
    }
    var projected: [(Data, Document)] = []
    var seen = Set<Data>()
    for value in source {
      guard case .document(let document) = value else {
        throw KwiftDBError.table("mirrored query contains a non-document row")
      }
      var supplied: [String: Value] = [:]
      for field in document where schema.columnsByName[field.name] != nil { supplied[field.name] = field.value }
      var fields: [DocumentField] = []
      for column in schema.columns {
        guard let cell = supplied[column.name] else {
          if !column.optional { throw KwiftDBError.invalidRow("mirror row is missing \(column.name)") }
          continue
        }
        if cell == .null {
          guard column.optional else { throw KwiftDBError.invalidRow("mirror row has Null for \(column.name)") }
          continue
        }
        guard column.type.accepts(cell) else { throw KwiftDBError.invalidRow("mirror field \(column.name) has the wrong type") }
        fields.append(DocumentField(column.name, cell))
      }
      let row = try Document(fields)
      for term in scope.equalities where row[term.column] != term.value {
        throw KwiftDBError.table("mirrored row falls outside its declared scope")
      }
      let key = try mirrorPrimaryKey(row, schema: schema)
      guard seen.insert(key).inserted else { throw KwiftDBError.table("mirrored rows contain a duplicate primary key") }
      projected.append((key, row))
    }
    return projected
  }

  private static func mirrorPrimaryValues(_ row: Document, schema: TableSchema) throws -> [Value] {
    try schema.primaryKey.map { name in
      guard let value = row[name] else { throw KwiftDBError.invalidRow("mirror row is missing primary key \(name)") }
      return value
    }
  }

  private static func mirrorPrimaryKey(_ row: Document, schema: TableSchema) throws -> Data {
    Data(try KeyEncoder.encodeTuple(mirrorPrimaryValues(row, schema: schema)))
  }

  private static func syncKey(_ value: Value) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("s"), value])
  }

  /// Renumbers the pending mutations to follow `applied`, in their order.
  ///
  /// The server refuses a mutation numbered past its applied number plus one,
  /// which happens when the server's record of this client went back, for
  /// example after its database was restored. The pending mutations were
  /// never applied there, so they follow `applied` and the next session sends
  /// them in order instead of being refused forever.
  private func renumberPending(after applied: DatabaseUInt) async throws {
    let waiting = pending.sorted(by: { $0.key < $1.key }).filter { $0.key > applied }
    guard let first = waiting.first, first.key != applied.incremented() else { return }
    var batch = WriteBatch()
    var renumbered: [DatabaseUInt: PendingMutation] = pending.filter { $0.key <= applied }
    var next = applied.incremented()
    for (old, mutation) in waiting {
      batch.delete(try Self.pendingKey(try Number(old.description)))
      batch.put(Array(try ValueCodec.encode(.document(try Document([
        DocumentField("name", .text(mutation.name)), DocumentField("arguments", mutation.arguments),
      ])))), for: try Self.pendingKey(try Number(next.description)))
      renumbered[next] = mutation
      events.append(.renumbered(from: try Number(old.description), to: try Number(next.description)))
      next = next.incremented()
    }
    batch.put(Array(try ValueCodec.encode(.number(try Number(next.description)))), for: try Self.syncKey(.text("next-mutation")))
    try await database.writeBatch(batch)
    pending = renumbered
    nextMutation = next
  }

  private static func pendingKey(_ number: Number) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("s"), .text("pending"), .number(number)])
  }

  private static func unsigned(_ number: Number) throws -> DatabaseUInt {
    guard let text = number.unsignedIntegerText else { throw KwiftDBError.corruptStorage("sync counter is not an unsigned integer") }
    return try DatabaseUInt(text)
  }

  private static func encodeRanges(_ ranges: [KeyRange]) throws -> [Value] {
    try normalized(ranges).map { range in
      .document(try Document([DocumentField("start", .bytes(range.start)), DocumentField("end", .bytes(range.end))]))
    }
  }

  private static func decodeRanges(_ value: Value) throws -> [KeyRange] {
    guard case .list(let values) = value else { throw KwiftDBError.corruptStorage("held sync ranges are not a list") }
    let ranges = try values.map { value -> KeyRange in
      guard case .document(let item) = value, case .bytes(let start)? = item["start"],
            case .bytes(let end)? = item["end"] else {
        throw KwiftDBError.corruptStorage("malformed held sync range")
      }
      return KeyRange(start: start, end: end)
    }
    return normalized(ranges)
  }

  private static func normalized(_ ranges: [KeyRange]) -> [KeyRange] { KeyRanges.normalized(ranges) }

  private static func subtract(_ ranges: [KeyRange], _ removed: [KeyRange]) -> [KeyRange] {
    KeyRanges.subtract(ranges, removed)
  }
}

private struct PendingMutation: Sendable {
  let name: String
  let arguments: Value
}

private struct ReplicaSubscription: Sendable {
  let name: String
  let arguments: Value
}
