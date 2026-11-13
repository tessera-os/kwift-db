import Foundation
import KwiftDB
import KwiftDBQUIC
import Testing

/// End-to-end checks of the QUIC sync transport (spec section 8.6): a real
/// `SyncHost` and `SyncLink` on loopback, each over its own database directory.
@Suite("Sync over QUIC")
struct SyncTransportTests {
  /// One shared note document: a query that reads it and a mutation that sets it.
  static let functions: Functions = try! Functions(
    queries: [
      QueryFunction(name: "note") { run, _, _ in
        let document = try await run.collection(named: "notes").document(for: .text("main"))
        return document?["text"] ?? .null
      }
    ],
    mutations: [
      MutationFunction(name: "set-note") { run, _, arguments in
        guard case .text = arguments else { throw FunctionFailure.refused("note must be text") }
        try await run.collection(named: "notes").put(
          try Document(fields: [DocumentField("text", arguments)]),
          for: .text("main")
        )
        return arguments
      }
    ]
  )

  @Test("a mutation sent over QUIC commits on the server and reaches the replica's query", .timeLimit(.minutes(1)))
  func mutationRoundTrip() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkMutationRoundTrip(idle: .zero)
  }

  @Test("a session left idle past the idle timeout stays connected", .timeLimit(.minutes(1)))
  func idleSessionStays() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkMutationRoundTrip(idle: SyncTLS.idleTimeout + .seconds(5))
  }

  /// Connects, stays quiet for `idle`, then round-trips a mutation; any
  /// disconnect on the way is an issue.
  @available(anyAppleOS 27, *)
  private func checkMutationRoundTrip(idle: Duration) async throws {
    let (certificate, trust) = try Self.loopbackTLS()

    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-sync-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    for folder in ["server", "replica"] {
      try FileManager.default.createDirectory(at: root.appending(path: folder), withIntermediateDirectories: true)
    }
    let serverDatabase = try Database(directory: root.appending(path: "server"))
    let server = SyncServer(database: serverDatabase, functions: Self.functions) { credentials in
      guard case .text(let device) = credentials, !device.isEmpty else { throw FunctionFailure.refused("device required") }
      return .text(device)
    }
    let host = try await SyncHost.bind(server: server, port: 0, certificate: certificate)
    let replica = try Replica(directory: root.appending(path: "replica"), functions: Self.functions)
    let link = SyncLink(replica: replica)

    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask { await host.run() }
      group.addTask { await link.run(host: "localhost", port: host.port, credentials: .text("test-device"), trust: trust) }
      defer { group.cancelAll() }
      try await checkRoundTrip(link: link, replica: replica, serverDatabase: serverDatabase, idle: idle)
    }
  }

  @available(anyAppleOS 27, *)
  private func checkRoundTrip(
    link: SyncLink, replica: Replica, serverDatabase: Database, idle: Duration
  ) async throws {
    var events = link.events.makeAsyncIterator()
    #expect(await next(&events) { if case .connected = $0 { true } else { false } } != nil)
    let welcomed = await next(&events) { if case .replica(.welcomed) = $0 { true } else { false } }
    guard case .replica(.welcomed(let identityValue))? = welcomed else {
      Issue.record("no welcome")
      return
    }
    #expect(identityValue == .text("test-device"))
    try await Task.sleep(for: idle)

    let query = try await replica.subscribe(name: "note", arguments: .null)
    let mutation = try await replica.mutate(name: "set-note", arguments: .text("hello over quic"))
    await link.send()

    // The answer and the query's new value arrive in the same transition, in
    // either order, so collect both.
    var outcome: SyncMutationOutcome?
    var noteSeen: Value?
    let deadline = ContinuousClock.now + .seconds(10)
    while (outcome == nil || noteSeen != .text("hello over quic")), ContinuousClock.now < deadline,
      let event = await events.next(isolation: #isolation)
    {
      switch event {
      case .replica(.done(let number, let result)) where number == mutation: outcome = result
      case .replica(.changed(let changed, let value)) where changed == query: noteSeen = value
      case .disconnected(let reason): Issue.record("disconnected: \(reason ?? "cleanly")")
      default: break
      }
    }
    #expect(outcome == .value(.text("hello over quic")))
    #expect(noteSeen == .text("hello over quic"))

    // The server's database of record holds the write.
    let stored = try await serverDatabase.collection(named: "notes").document(for: .text("main"))
    #expect(stored?["text"] == .text("hello over quic"))
  }

  @Test("a session the server refuses ends with the refusal", .timeLimit(.minutes(1)))
  func refusedSession() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkRefusedSession()
  }

  @available(anyAppleOS 27, *)
  private func checkRefusedSession() async throws {
    let (certificate, trust) = try Self.loopbackTLS()
    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-sync-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    for folder in ["server", "replica"] {
      try FileManager.default.createDirectory(at: root.appending(path: folder), withIntermediateDirectories: true)
    }

    let server = SyncServer(database: try Database(directory: root.appending(path: "server")), functions: Self.functions) { _ in
      throw FunctionFailure.refused("device required")
    }
    let host = try await SyncHost.bind(server: server, port: 0, certificate: certificate)
    let link = SyncLink(replica: try Replica(directory: root.appending(path: "replica"), functions: Self.functions))

    await withDiscardingTaskGroup { group in
      group.addTask { await host.run() }
      // The link's session ends by itself once the server refuses it.
      await link.run(host: "localhost", port: host.port, credentials: .null, trust: trust)
      group.cancelAll()
    }
    var events = link.events.makeAsyncIterator()
    let refused = await next(&events) { if case .replica(.refused) = $0 { true } else { false } }
    guard case .replica(.refused(let reason))? = refused else {
      Issue.record("session was not refused")
      return
    }
    #expect(reason == "device required")
  }

  @Test("pending mutations the server refuses as out of order reach the replica, which renumbers them", .timeLimit(.minutes(1)))
  func outOfOrderRecovers() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkOutOfOrderRecovers()
  }

  /// A server whose record of the client went back, as after a restore. The
  /// replica pipelines two pending mutations past it: the `refused` frame must
  /// arrive before the connection closes, or the replica never renumbers.
  @available(anyAppleOS 27, *)
  private func checkOutOfOrderRecovers() async throws {
    let (certificate, trust) = try Self.loopbackTLS()
    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-sync-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    for folder in ["first", "restored", "replica"] {
      try FileManager.default.createDirectory(at: root.appending(path: folder), withIntermediateDirectories: true)
    }
    let first = SyncServer(database: try Database(directory: root.appending(path: "first")), functions: Self.functions) { _ in .text("device") }
    let restored = SyncServer(database: try Database(directory: root.appending(path: "restored")), functions: Self.functions) { _ in .text("device") }
    let replica = try Replica(directory: root.appending(path: "replica"), functions: Self.functions)
    _ = try await replica.subscribe(name: "note", arguments: .null)

    // Two mutations applied by the first server, in process.
    _ = try await replica.mutate(name: "set-note", arguments: .text("one"))
    _ = try await replica.mutate(name: "set-note", arguments: .text("two"))
    let messages = try await replica.connect(credentials: .null)
    guard case .hello(let client, let credentials)? = messages.first,
          case .accepted(let connection, let welcome) = await first.connect(client: client, credentials: credentials)
    else { Issue.record("not accepted"); return }
    _ = try await replica.receive(welcome)
    for message in messages.dropFirst() {
      for reply in try await first.receive(message, from: connection) { _ = try await replica.receive(reply) }
    }
    try await replica.disconnect()
    #expect(try await replica.mutate(name: "set-note", arguments: .text("three")) == Number(3))
    #expect(try await replica.mutate(name: "set-note", arguments: .text("four")) == Number(4))

    let host = try await SyncHost.bind(server: restored, port: 0, certificate: certificate)
    await withDiscardingTaskGroup { group in
      group.addTask { await host.run() }
      defer { group.cancelAll() }

      let refusedLink = SyncLink(replica: replica)
      await refusedLink.run(host: "localhost", port: host.port, credentials: .null, trust: trust)
      var refusals: [String] = []
      var moves: [(Number, Number)] = []
      // A link's events never finish, since it can run again; the session's
      // events end with `disconnected`.
      for await event in refusedLink.events {
        if case .disconnected = event { break }
        if case .replica(.refused(let reason)) = event { refusals.append(reason) }
        if case .replica(.renumbered(let old, let new)) = event { moves.append((old, new)) }
      }
      #expect(refusals == ["mutation out of order"])
      #expect(moves.map { $0.0 } == [Number(3), Number(4)])
      #expect(moves.map { $0.1 } == [Number(1), Number(2)])

      let link = SyncLink(replica: replica)
      group.addTask { await link.run(host: "localhost", port: host.port, credentials: .null, trust: trust) }
      var events = link.events.makeAsyncIterator()
      #expect(await next(&events) { if case .replica(.done(Number(1), .value(.text("three")))) = $0 { true } else { false } } != nil)
      #expect(await next(&events) { if case .replica(.done(Number(2), .value(.text("four")))) = $0 { true } else { false } } != nil)
    }
  }

  /// The loopback certificate the host presents and the root the link trusts.
  private static func loopbackTLS() throws -> (SyncHostCertificate, SyncTrust) {
    let fixtures = try #require(Bundle.module.url(forResource: "Fixtures", withExtension: nil))
    let certificate = SyncHostCertificate(
      serverName: "localhost",
      chainPath: fixtures.appending(path: "loopback-chain.pem").path,
      privateKeyPath: fixtures.appending(path: "loopback-key.pem").path
    )
    return (certificate, .roots(path: fixtures.appending(path: "loopback-root.pem").path))
  }

  /// Returns the first event matching `predicate`, or nil after ten seconds.
  @available(anyAppleOS 27, *)
  private func next(
    _ events: inout AsyncStream<SyncLinkEvent>.Iterator,
    where predicate: (SyncLinkEvent) -> Bool
  ) async -> SyncLinkEvent? {
    let deadline = ContinuousClock.now + .seconds(10)
    while ContinuousClock.now < deadline {
      guard let event = await events.next(isolation: #isolation) else { return nil }
      if case .disconnected(let reason?) = event { Issue.record("disconnected: \(reason)") }
      if predicate(event) { return event }
    }
    return nil
  }
}
