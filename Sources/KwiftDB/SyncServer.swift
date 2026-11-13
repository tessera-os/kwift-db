import Foundation
import Logging

/// The result of authenticating and opening one sync session.
public enum SyncConnectionResult: Sendable {
  /// The session is open and should send this welcome before client messages.
  case accepted(connection: UUID, welcome: SyncMessage)
  /// Authentication failed and this message ends the attempted session.
  case refused(SyncMessage)
}

/// An in-process sync server engine independent of its network transport.
///
/// The server authenticates clients, records subscription reads, serializes
/// mutation application through one actor, and creates protocol transitions.
/// A transport reads messages with `receive` and drains all queued messages
/// with `drain`; Hummingbird or another byte-stream layer can own sockets
/// without moving persistence or query behavior out of this engine.
///
/// With a `ReplicationCoordinator` the server is one member of a replication
/// group (spec section 9): the leader runs every mutation and every `change`,
/// a follower forwards them, and every member applies each committed entry
/// and then refreshes its own sessions. Run `run()` beside the transports.
public actor SyncServer {
  /// Resolves credentials to an identity or throws a refusal reason.
  public typealias Authenticate = @Sendable (Value) async throws -> Value

  let database: Database
  let functions: Functions
  let authenticate: Authenticate
  let replication: ReplicationCoordinator?
  var sessions: [UUID: ServerSession] = [:]
  private var processing = false
  private var processingWaiters: [CheckedContinuation<Void, Never>] = []
  private let outboxContinuation: AsyncStream<UUID>.Continuation
  let commitContinuation: AsyncStream<Number>.Continuation

  /// Connections whose outbox gained messages that no `receive` call returned.
  ///
  /// A mutation from one client, or a server-owned `streamChange`, can queue
  /// transitions for every other session. Those sessions sent nothing, so no
  /// `receive` call hands their messages back. A transport consumes this
  /// sequence and calls `drain` for each connection it yields. A connection may
  /// be yielded when its outbox is already empty; `drain` then returns nothing.
  public nonisolated let outboxSignals: AsyncStream<UUID>

  /// The database version after each committed batch: every mutation that
  /// wrote, every `change` and every `streamChange`.
  ///
  /// Server-side workers wait on this instead of polling. It keeps only the
  /// newest version, so a slow consumer sees the latest state, not every step.
  public nonisolated let commits: AsyncStream<Number>

  /// Creates a server over the database of record and shared functions.
  ///
  /// - Parameter replication: The member's replication state, when the server
  ///   is one of a group. `database` must be the database that member
  ///   replicates, and nothing else may write it.
  public init(
    database: Database,
    functions: Functions,
    replication: ReplicationCoordinator? = nil,
    authenticate: @escaping Authenticate
  ) {
    self.database = database
    self.functions = functions
    self.replication = replication
    self.authenticate = authenticate
    (outboxSignals, outboxContinuation) = AsyncStream.makeStream(of: UUID.self, bufferingPolicy: .unbounded)
    (commits, commitContinuation) = AsyncStream.makeStream(of: Number.self, bufferingPolicy: .bufferingNewest(1))
  }

  /// Authenticates credentials and opens a new session for a durable client id.
  public func connect(client requestedClient: Number, credentials: Value) async -> SyncConnectionResult {
    await acquireProcessing()
    let identity: Value
    do { identity = try await authenticate(credentials) }
    catch {
      releaseProcessing()
      return .refused(.refused(reason: Self.failureText(error)))
    }

    do {
      let client: Number
      if requestedClient.isZero {
        client = try await takeNextClient()
      } else {
        client = requestedClient
      }

      // A reconnect replaces any older in-process session for this client.
      let replaced = sessions.compactMap { $0.value.client == client ? $0.key : nil }
      for id in replaced { sessions.removeValue(forKey: id) }
      let appliedKey = try Self.appliedKey(client)
      let applied = try await readNumber(appliedKey) ?? .zero
      let connection = UUID()
      sessions[connection] = ServerSession(client: client, identity: identity, applied: applied)
      releaseProcessing()
      return .accepted(connection: connection, welcome: .welcome(client: client, identity: identity))
    } catch {
      releaseProcessing()
      return .refused(.refused(reason: Self.failureText(error)))
    }
  }

  /// Processes one client message and returns messages now queued for it.
  public func receive(_ message: SyncMessage, from connection: UUID) async throws -> [SyncMessage] {
    if case .heartbeat = message { return [] }
    if replication != nil, case .mutate(let number, let name, let arguments) = message {
      return try await receiveReplicated(number: number, name: name, arguments: arguments, connection: connection)
    }
    let clock = ContinuousClock()
    let arrived = clock.now
    await acquireProcessing()
    let started = clock.now
    do {
      guard sessions[connection] != nil else { throw KwiftDBError.table("sync session is closed") }
      switch message {
      case .subscribe(let query, let name, let arguments):
        try await subscribe(query: query, name: name, arguments: arguments, connection: connection)
      case .unsubscribe(let query):
        try await unsubscribe(query: query, connection: connection)
      case .mutate(let number, let name, let arguments):
        try await mutate(number: number, name: name, arguments: arguments, connection: connection)
      case .heartbeat:
        break
      case .hello, .welcome, .refused, .transition:
        sessions[connection]?.outbox.append(.refused(reason: "malformed message"))
        sessions[connection]?.closed = true
      }
      let messages = drain(connection)
      releaseProcessing()
      Self.log(message, waited: started - arrived, worked: clock.now - started, answer: messages)
      return messages
    } catch {
      releaseProcessing()
      Self.log(message, waited: started - arrived, worked: clock.now - started, answer: [])
      throw error
    }
  }

  /// Takes all queued transition or refusal messages for one connection.
  public func drain(_ connection: UUID) -> [SyncMessage] {
    guard var session = sessions[connection] else { return [] }
    let messages = session.outbox
    session.outbox.removeAll(keepingCapacity: true)
    if session.closed { sessions.removeValue(forKey: connection) }
    else { sessions[connection] = session }
    return messages
  }

  /// Ends a session and releases its in-memory subscriptions.
  public func disconnect(_ connection: UUID) async {
    await acquireProcessing()
    sessions.removeValue(forKey: connection)
    releaseProcessing()
  }

  /// Writes a server-owned append-only stream update, then re-runs readers.
  public func streamChange(_ operation: @Sendable (DurableStreams) async throws -> Void) async throws {
    if let replication {
      try await replicatedChange(through: replication) { run in try await operation(DurableStreams(session: run)) }
      return
    }
    await acquireProcessing()
    do {
      let streams = DurableStreams(session: database)
      let before = try await database.version()
      try await operation(streams)
      let after = try await database.version()
      if after != before {
        try await refreshAllHeldRanges()
        commitContinuation.yield(after)
      }
      releaseProcessing()
    } catch {
      releaseProcessing()
      throw error
    }
  }

  /// Runs a server-owned change and broadcasts it like a mutation's writes.
  ///
  /// The operation reads and writes through one recorded run: documents,
  /// tables and durable streams alike. Its writes commit as one batch when it
  /// returns, and every subscription that read a written key runs again, as
  /// after a client mutation (spec section 8.4). When it throws, nothing is
  /// written and its error is rethrown.
  ///
  /// This is how server code that is not a client, such as an AI worker
  /// recording a reply, changes data that clients subscribe to. Writing the
  /// database directly would leave every replica stale.
  ///
  /// - Returns: The operation's answer.
  ///
  /// On a replicated server `change` is leader-only: a mutation runs on the
  /// leader against its database of record, one at a time (spec section 9.6),
  /// and a follower's copy may trail it. The leader runs the operation and
  /// appends its writes to the log, and `change` returns once this member has
  /// applied the entry. On a follower it throws `ReplicationError.notLeader`,
  /// and with no leader known `ReplicationError.unavailable`. Code that may run
  /// on any member calls `mutate(_:arguments:as:)`, which forwards.
  public func change<T: Sendable>(_ operation: @Sendable (RecordedRun) async throws -> T) async throws -> T {
    if let replication { return try await replicatedChange(through: replication, operation) }
    await acquireProcessing()
    defer { releaseProcessing() }
    let result = try await database.runRecorded(operation)
    switch result.outcome {
    case .success(let value):
      if !result.writes.isEmpty {
        await refreshSubscriptions(writes: result.writes)
        signalPendingOutboxes()
        commitContinuation.yield(try await database.version())
      }
      return value
    case .failure(let error):
      throw error
    }
  }

  /// Runs a mutation function against the database without committing, and
  /// answers its outcome with the writes to commit for it.
  ///
  /// A function with server access does not exist for a client caller.
  func runMutation(
    named name: String, arguments: Value, caller: Caller, calledByClient: Bool = true
  ) async throws -> (SyncMutationOutcome, [WriteOperation]) {
    guard let function = functions.mutation(named: name), !calledByClient || function.access == .client else {
      return (.failure(FunctionFailure.noSuchFunction(name).localizedDescription), [])
    }
    let result = try await database.runRecorded(commitWrites: false) { run in
      try await function.run(run, caller, arguments)
    }
    switch result.outcome {
    case .success(let value): return (.value(value), result.writes)
    case .failure: return (.failure(result.outcome.failureText ?? ""), [])
    }
  }

  private func subscribe(query: Number, name: String, arguments: Value, connection: UUID) async throws {
    guard var session = sessions[connection] else { return }
    let oldRanges = session.held
    let subscription = await runQuery(name: name, arguments: arguments, client: session.client, identity: session.identity)
    session.subscriptions[query] = ServerSubscription(name: name, arguments: arguments, ranges: subscription.ranges)
    session.held = Self.normalized(session.subscriptions.values.flatMap(\.ranges))
    sessions[connection] = session
    let changes = try await changes(from: oldRanges, to: session.held, writes: [], includePairs: true)
    let version = try await database.version()
    let ready = [SyncReady(query: query)]
    sessions[connection]?.outbox.append(.transition(version: version, applied: session.applied, changes: changes, ready: ready, results: []))
  }

  private func unsubscribe(query: Number, connection: UUID) async throws {
    guard var session = sessions[connection] else { return }
    let oldRanges = session.held
    session.subscriptions.removeValue(forKey: query)
    session.held = Self.normalized(session.subscriptions.values.flatMap(\.ranges))
    sessions[connection] = session
    let changes = try await changes(from: oldRanges, to: session.held, writes: [], includePairs: true)
    let version = try await database.version()
    if !changes.isEmpty {
      sessions[connection]?.outbox.append(.transition(version: version, applied: session.applied, changes: changes, ready: [], results: []))
    }
  }

  private func mutate(number: Number, name: String, arguments: Value, connection: UUID) async throws {
    guard let callerSession = sessions[connection] else { return }
    let appliedKey = try Self.appliedKey(callerSession.client)
    let applied = try await readNumber(appliedKey) ?? .zero
    let requestNumber = try Self.unsigned(number)
    let appliedNumber = try Self.unsigned(applied)
    if requestNumber <= appliedNumber {
      let version = try await database.version()
      sessions[connection]?.outbox.append(.transition(
        version: version, applied: applied, changes: [], ready: [], results: [(number, .skipped)]
      ))
      return
    }
    guard requestNumber == appliedNumber.incremented() else {
      sessions[connection]?.outbox.append(.refused(reason: "mutation out of order"))
      sessions[connection]?.closed = true
      return
    }

    let caller = Caller(client: callerSession.client, identity: callerSession.identity)
    let (outcome, mutationWrites) = try await runMutation(named: name, arguments: arguments, caller: caller)
    var batch = WriteBatch(mutationWrites)
    batch.put(Array(try ValueCodec.encode(.number(number))), for: appliedKey)
    try await database.writeBatch(batch)
    guard var updatedCaller = sessions[connection] else { return }
    updatedCaller.applied = number
    sessions[connection] = updatedCaller
    var committedWrites = batch.operations
    committedWrites = committedWrites.filter { operation in
      if case .put(let key, _) = operation { return key != appliedKey }
      return operation.key != appliedKey
    }
    await refreshSubscriptions(writes: committedWrites, result: (connection, number, outcome))
    signalPendingOutboxes(except: connection)
    if !committedWrites.isEmpty { commitContinuation.yield(try await database.version()) }
  }

  /// Yields every session, other than the one whose `receive` call returns its
  /// own messages, that now has queued messages.
  func signalPendingOutboxes(except excluded: UUID? = nil) {
    for (connection, session) in sessions where connection != excluded && !session.outbox.isEmpty {
      outboxContinuation.yield(connection)
    }
  }

  private func runQuery(name: String, arguments: Value, client: Number, identity: Value) async -> (ranges: [KeyRange], failure: String?) {
    guard let function = functions.query(named: name) else {
      return ([], FunctionFailure.noSuchFunction(name).localizedDescription)
    }
    do {
      let result = try await database.runRecorded(commitWrites: false, readOnly: true) { run in
        try await function.run(run, Caller(client: client, identity: identity), arguments)
      }
      switch result.outcome {
      case .success: return (Self.normalized(result.ranges), nil)
      case .failure: return (Self.normalized(result.ranges), result.outcome.failureText)
      }
    } catch { return ([], Self.failureText(error)) }
  }

  func refreshSubscriptions(
    writes: [WriteOperation],
    result: (UUID, Number, SyncMutationOutcome)? = nil,
    force: Bool = false
  ) async {
    for connection in Array(sessions.keys) {
      guard var session = sessions[connection] else { continue }
      let oldRanges = session.held
      for query in Array(session.subscriptions.keys) {
        guard let subscription = session.subscriptions[query], force ||
              writes.contains(where: { KeyRanges.contain(subscription.ranges, $0.key) }) else { continue }
        let evaluated = await runQuery(
          name: subscription.name,
          arguments: subscription.arguments,
          client: session.client,
          identity: session.identity
        )
        session.subscriptions[query]?.ranges = evaluated.ranges
      }
      session.held = Self.normalized(session.subscriptions.values.flatMap(\.ranges))
      sessions[connection] = session
        let resultForSession = result.flatMap { $0.0 == connection ? ($0.1, $0.2) : nil }
      do {
        let changes = try await changes(from: oldRanges, to: session.held, writes: writes, includePairs: true)
        guard !changes.isEmpty || resultForSession != nil else { continue }
        let version = try await database.version()
        let results = resultForSession.map { [$0] } ?? []
        sessions[connection]?.outbox.append(.transition(
          version: version, applied: session.applied, changes: changes, ready: [], results: results
        ))
      } catch {
        sessions[connection]?.outbox.append(.refused(reason: Self.failureText(error)))
        sessions[connection]?.closed = true
      }
    }
  }

  private func changes(
    from old: [KeyRange],
    to new: [KeyRange],
    writes: [WriteOperation],
    includePairs: Bool
  ) async throws -> [SyncChange] {
    var result: [SyncChange] = []
    for operation in writes {
      let key = operation.key
      guard KeyRanges.contain(old, key), KeyRanges.contain(new, key) else { continue }
      switch operation {
      case .put(let key, let value): result.append(.write(key: key, value: value))
      case .delete(let key): result.append(.write(key: key, value: nil))
      }
    }
    for range in Self.subtract(new, old) {
      let pairs = includePairs ? try await database.scanKeys(from: range.start, to: range.end) : []
      result.append(.hold(range: range, pairs: pairs))
    }
    for range in Self.subtract(old, new) { result.append(.release(range)) }
    return result
  }

  func readNumber(_ key: [UInt8]) async throws -> Number? {
    guard let bytes = try await database.readKey(key) else { return nil }
    guard case .number(let number) = try ValueCodec.decode(Data(bytes)) else {
      throw KwiftDBError.corruptStorage("sync state is not a number")
    }
    return number
  }

  static func syncKey(_ value: Value) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("s"), value])
  }

  static func appliedKey(_ client: Number) throws -> [UInt8] {
    try KeyEncoder.encodeTuple([.text("s"), .text("applied"), .number(client)])
  }

  static func unsigned(_ number: Number) throws -> DatabaseUInt {
    guard let text = number.unsignedIntegerText else { throw KwiftDBError.corruptStorage("sync counter is not an unsigned integer") }
    return try DatabaseUInt(text)
  }

  static func failureText(_ error: any Error) -> String {
    (error as? any LocalizedError)?.errorDescription ?? String(describing: error)
  }

  static func normalized(_ ranges: [KeyRange]) -> [KeyRange] { KeyRanges.normalized(ranges) }

  private static func subtract(_ ranges: [KeyRange], _ removed: [KeyRange]) -> [KeyRange] {
    KeyRanges.subtract(ranges, removed)
  }

  /// Logs a client message: how long it waited for the server's other work,
  /// how long its own took (the query, or the mutation with every
  /// subscription it refreshed), and how many rows the answer carried. A
  /// message that took 50 ms or more is logged at `info`, the rest at `debug`.
  private static func log(_ message: SyncMessage, waited: Duration, worked: Duration, answer: [SyncMessage]) {
    let slow = waited + worked >= .milliseconds(50)
    guard slow || logger.logLevel <= .debug else { return }
    let what: String
    switch message {
    case .subscribe(_, let name, _): what = "subscribe \(name)"
    case .mutate(_, let name, _): what = "mutate \(name)"
    case .unsubscribe: what = "unsubscribe"
    default: what = "message"
    }
    var rows = 0
    var refused: String?
    var outcomes: [String] = []
    for sent in answer {
      switch sent {
      case .transition(_, _, let changes, _, let results):
        for (number, outcome) in results {
          switch outcome {
          case .value: outcomes.append("\(number)=done")
          case .failure(let reason): outcomes.append("\(number)=failed(\(reason))")
          case .skipped: outcomes.append("\(number)=skipped")
          }
        }
        for change in changes {
          switch change {
          case .write: rows += 1
          case .hold(_, let pairs): rows += pairs.count
          case .release: break
          }
        }
      case .refused(let reason): refused = reason
      default: break
      }
    }
    var metadata: Logger.Metadata = [
      "message": "\(what)",
      "wait_ms": "\(waited.milliseconds)",
      "work_ms": "\(worked.milliseconds)",
      "rows": "\(rows)",
    ]
    if let refused { metadata["refused"] = "\(refused)" }
    if case .mutate(let number, _, _) = message {
      metadata["mutation"] = "\(number)"
      metadata["outcomes"] = "\(outcomes.joined(separator: " "))"
    }
    logger.log(level: slow ? .info : .debug, slow ? "slow sync message" : "sync message", metadata: metadata)
  }

  private static let logger = Logger(label: "kwiftdb.sync.server")

  func acquireProcessing() async {
    if !processing { processing = true; return }
    await withCheckedContinuation { processingWaiters.append($0) }
  }

  func releaseProcessing() {
    if processingWaiters.isEmpty { processing = false }
    else { processingWaiters.removeFirst().resume() }
  }
}

struct ServerSession {
  let client: Number
  let identity: Value
  var applied = Number.zero
  var subscriptions: [Number: ServerSubscription] = [:]
  var held: [KeyRange] = []
  var outbox: [SyncMessage] = []
  var closed = false
}

struct ServerSubscription {
  let name: String
  let arguments: Value
  var ranges: [KeyRange]
}
