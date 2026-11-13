import Foundation

/// The replicated half of `SyncServer` (spec section 9.6).
///
/// Every instance does two things with the log. A mutation or `change` is
/// proposed to the leader, which runs it and appends an entry. Every instance,
/// the leader included, then applies the entry and refreshes its own sessions
/// from it, so a client sees every write whichever instance it is attached to.
extension SyncServer {
  /// The failure text of a mutation that arrives out of order (spec section 8.4).
  static let outOfOrder = "mutation out of order"

  /// The live members of the group (spec section 9.5): the ones the leader
  /// heard from lately, this one included. Empty when the server is not
  /// replicated. A server-side worker uses it to share work by who is up.
  public var liveMembers: [Member] {
    get async { await replication?.liveMembers ?? [] }
  }

  /// The live members each time the list changes. One consumer. Finished at
  /// once when the server is not replicated.
  public nonisolated var liveMemberChanges: AsyncStream<[Member]> {
    replication?.liveMemberChanges ?? AsyncStream { $0.finish() }
  }

  /// Applies committed log entries and serves forwarded requests until the
  /// calling task is cancelled. Does nothing, and returns, when the server is
  /// not replicated.
  ///
  /// Run it beside the transports: the replication host feeds the log, this
  /// loop turns each applied entry into transitions for the sessions of this
  /// instance.
  public nonisolated func run() async {
    guard let replication else { return }
    await withTaskGroup(of: Void.self) { group in
      group.addTask { for await change in replication.changes { await self.apply(change) } }
      group.addTask { await self.serveForwards(from: replication) }
    }
  }

  // MARK: Applying

  /// Refreshes sessions for one applied change.
  private func apply(_ change: ReplicatedChange) async {
    await acquireProcessing()
    defer { releaseProcessing() }
    switch change {
    case .snapshot:
      try? await refreshAllHeldRanges()
      if let version = try? await database.version() { commitContinuation.yield(version) }
    case .entry(let entry):
      guard let batch = try? entry.writeBatch() else { return }
      var writes = batch.operations
      var result: (UUID, Number, SyncMutationOutcome)?
      if case .mutation(let client, let number, let applied, let outcome)? = try? ReplicatedNote(notes: entry.notes) {
        if let key = try? Self.appliedKey(client) { writes.removeAll { $0.key == key } }
        if let connection = sessions.first(where: { $0.value.client == client })?.key {
          if let current = sessions[connection]?.applied, let a = try? Self.unsigned(applied), let c = try? Self.unsigned(current), a > c {
            sessions[connection]?.applied = applied
          }
          result = (connection, number, outcome)
        }
      }
      guard !writes.isEmpty || result != nil else { return }
      await refreshSubscriptions(writes: writes, result: result)
      signalPendingOutboxes()
      if !writes.isEmpty, let version = try? await database.version() { commitContinuation.yield(version) }
    }
  }

  /// Re-runs every subscription and sends each session its held ranges
  /// again, for changes that are not a list of writes: a server-owned stream
  /// update or a replaced database. The processing gate must be held.
  func refreshAllHeldRanges() async throws {
    var visibleWrites: [WriteOperation] = []
    for range in Self.normalized(sessions.values.flatMap(\.held)) {
      for pair in try await database.scanKeys(from: range.start, to: range.end) {
        visibleWrites.append(.put(key: pair.key, value: pair.value))
      }
    }
    await refreshSubscriptions(writes: visibleWrites, force: true)
    signalPendingOutboxes()
  }

  // MARK: Proposing

  /// A client's mutation on a replicated server: the leader runs it here, a
  /// follower forwards it. The result reaches the session when the entry is
  /// applied, through `apply`.
  func receiveReplicated(number: Number, name: String, arguments: Value, connection: UUID) async throws -> [SyncMessage] {
    guard let replication, let session = sessions[connection] else { throw KwiftDBError.table("sync session is closed") }
    let request = MutationRequest(client: session.client, identity: session.identity, number: number, name: name, arguments: arguments)
    do {
      if await replication.status().role == .leader {
        _ = try await runLeaderMutation(request, through: replication)
      } else {
        _ = try await replication.forward(WriteBatch(), notes: ReplicatedRequest.mutation(request).notes)
      }
    } catch ReplicationError.refused(let reason) where reason == Self.outOfOrder {
      sessions[connection]?.outbox.append(.refused(reason: reason))
      sessions[connection]?.closed = true
    }
    return drain(connection)
  }

  /// Runs a mutation as the leader and appends its entry (spec sections 8.4, 9.6).
  private func runLeaderMutation(_ request: MutationRequest, through replication: ReplicationCoordinator) async throws -> ReplicationEntry {
    await acquireProcessing()
    defer { releaseProcessing() }
    try await replication.awaitLeaderReady()
    let appliedKey = try Self.appliedKey(request.client)
    let applied = try await readNumber(appliedKey) ?? .zero
    let requested = try Self.unsigned(request.number)
    let current = try Self.unsigned(applied)
    var batch = WriteBatch()
    let note: ReplicatedNote
    if requested <= current {
      note = .mutation(client: request.client, number: request.number, applied: applied, outcome: .skipped)
    } else if requested == current.incremented() {
      let caller = Caller(client: request.client, identity: request.identity)
      let (outcome, writes) = try await runMutation(named: request.name, arguments: request.arguments, caller: caller)
      batch = WriteBatch(writes)
      batch.put(Array(try ValueCodec.encode(.number(request.number))), for: appliedKey)
      note = .mutation(client: request.client, number: request.number, applied: request.number, outcome: outcome)
    } else {
      throw ReplicationError.refused(Self.outOfOrder)
    }
    return try await replication.appendAsLeader(batch, notes: note.notes)
  }

  /// Calls a registered mutation from server code, on the leader (spec section 9.6).
  ///
  /// It is how a server-side worker changes data by name, whichever member it
  /// runs on. The leader runs the mutation against its database of record, one
  /// at a time like a client's mutation. A follower forwards the call, and the
  /// answer comes back through the entry's notes, so `mutate` returns once this
  /// member has applied the leader's entry and the caller reads its own writes.
  /// Functions with `MutationFunction.Access.server` are callable only this way.
  /// The mutation runs as `identity` with client id zero; it carries no client
  /// numbering, so it is not deduplicated: a retry after a failure that left the
  /// outcome unknown may run it twice.
  ///
  /// - Returns: The mutation's answer.
  /// - Throws: `FunctionFailure.noSuchFunction` for an unregistered name;
  ///   `FunctionFailure.refused` carrying the text of a failure of the
  ///   mutation; `ReplicationError.unavailable` when no leader is known.
  public func mutate(_ name: String, arguments: Value = .null, as identity: Value) async throws -> Value {
    guard let function = functions.mutation(named: name) else { throw FunctionFailure.noSuchFunction(name) }
    guard let replication else {
      return try await change { run in
        try await function.run(run, Caller(client: .zero, identity: identity), arguments)
      }
    }
    let entry: ReplicationEntry
    if await replication.status().role == .leader {
      entry = try await runLeaderServerMutation(name: name, arguments: arguments, identity: identity, through: replication)
    } else {
      entry = try await replication.forward(
        WriteBatch(), notes: ReplicatedRequest.serverMutation(name: name, arguments: arguments, identity: identity).notes
      )
    }
    guard case .serverMutation(let outcome)? = try ReplicatedNote(notes: entry.notes) else {
      throw KwiftDBError.corruptStorage("a server mutation was answered without an outcome")
    }
    switch outcome {
    case .value(let value): return value
    case .failure(let reason): throw FunctionFailure.refused(reason)
    case .skipped: throw KwiftDBError.corruptStorage("a server mutation was skipped")
    }
  }

  private func runLeaderServerMutation(
    name: String, arguments: Value, identity: Value, through replication: ReplicationCoordinator
  ) async throws -> ReplicationEntry {
    await acquireProcessing()
    defer { releaseProcessing() }
    try await replication.awaitLeaderReady()
    let (outcome, writes) = try await runMutation(
      named: name, arguments: arguments, caller: Caller(client: .zero, identity: identity), calledByClient: false
    )
    return try await replication.appendAsLeader(WriteBatch(writes), notes: ReplicatedNote.serverMutation(outcome: outcome).notes)
  }

  /// Takes the next client id (spec section 8.4) through the log. The
  /// processing gate must be held.
  func takeNextClient() async throws -> Number {
    guard let replication else {
      let nextKey = try Self.syncKey(.text("next-client"))
      let nextID = try Self.unsigned(try await readNumber(nextKey) ?? Number("1"))
      var batch = WriteBatch()
      batch.put(Array(try ValueCodec.encode(.number(try Number(nextID.incremented().description)))), for: nextKey)
      try await database.writeBatch(batch)
      return try Number(nextID.description)
    }
    if await replication.status().role != .leader {
      let entry = try await replication.forward(WriteBatch(), notes: ReplicatedRequest.allocation.notes)
      guard case .allocation(let client)? = try ReplicatedNote(notes: entry.notes) else {
        throw KwiftDBError.corruptStorage("client id request was answered without an id")
      }
      return client
    }
    return try await allocateClientAsLeader(through: replication).client
  }

  private func allocateClientAsLeader(through replication: ReplicationCoordinator) async throws -> (client: Number, entry: ReplicationEntry) {
    try await replication.awaitLeaderReady()
    let nextKey = try Self.syncKey(.text("next-client"))
    let nextID = try Self.unsigned(try await readNumber(nextKey) ?? Number("1"))
    let client = try Number(nextID.description)
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.number(try Number(nextID.incremented().description)))), for: nextKey)
    let entry = try await replication.appendAsLeader(batch, notes: ReplicatedNote.allocation(client: client).notes)
    return (client, entry)
  }

  /// `change` and `streamChange` on a replicated server.
  func replicatedChange<T: Sendable>(
    through replication: ReplicationCoordinator,
    _ operation: @Sendable (RecordedRun) async throws -> T
  ) async throws -> T {
    let status = await replication.status()
    guard status.role == .leader else {
      throw status.leader.map { ReplicationError.notLeader(leader: $0) } ?? .unavailable
    }
    await acquireProcessing()
    defer { releaseProcessing() }
    try await replication.awaitLeaderReady()
    let result = try await database.runRecorded(commitWrites: false, operation)
    switch result.outcome {
    case .failure(let error): throw error
    case .success(let value):
      guard !result.writes.isEmpty else { return value }
      _ = try await replication.appendAsLeader(WriteBatch(result.writes), notes: [])
      return value
    }
  }

  // MARK: Serving

  /// Answers the requests followers forward, one at a time, in arrival order.
  private func serveForwards(from replication: ReplicationCoordinator) async {
    for await request in replication.forwardRequests {
      let reply: ForwardedReply
      do {
        reply = .committed(index: try await serve(request, through: replication).index)
      } catch let error as ReplicationError {
        reply = .failed(error)
      } catch {
        reply = .failed(.refused(Self.failureText(error)))
      }
      await replication.respond(to: request, with: reply)
    }
  }

  private func serve(_ request: ForwardedRequest, through replication: ReplicationCoordinator) async throws -> ReplicationEntry {
    switch try ReplicatedRequest(notes: request.notes) {
    case .mutation(let mutation)?:
      return try await runLeaderMutation(mutation, through: replication)
    case .serverMutation(let name, let arguments, let identity)?:
      return try await runLeaderServerMutation(name: name, arguments: arguments, identity: identity, through: replication)
    case .allocation?:
      await acquireProcessing()
      defer { releaseProcessing() }
      return try await allocateClientAsLeader(through: replication).entry
    case nil:
      await acquireProcessing()
      defer { releaseProcessing() }
      try await replication.awaitLeaderReady()
      return try await replication.appendAsLeader(request.batch, notes: request.notes)
    }
  }
}
