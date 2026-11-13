import Foundation
import KwiftDB
import KwiftDBQUIC
import Testing

/// A group of three members on loopback, replicating over QUIC (spec section 9)
/// with sync clients attached to different members (section 8).
@Suite("Replication over QUIC")
struct ReplicationTransportTests {
  /// The sync fixtures' functions plus a mutation only server code may call.
  static let functions: Functions = try! Functions(
    queries: SyncTransportTests.functions.queries,
    mutations: SyncTransportTests.functions.mutations + [
      MutationFunction(name: "server-set-note", access: .server) { run, _, arguments in
        try await run.collection(named: "notes").put(try Document(fields: [DocumentField("text", arguments)]), for: .text("main"))
        return arguments
      }
    ]
  )

  /// What an outdated or tampered client registers: the server-only mutation
  /// as if clients could call it, so the server's refusal is what stops it.
  static let tamperedClientFunctions: Functions = try! Functions(
    queries: SyncTransportTests.functions.queries,
    mutations: SyncTransportTests.functions.mutations + [
      MutationFunction(name: "server-set-note") { _, _, arguments in arguments }
    ]
  )

  @Test("three members elect one leader and agree on it, and the leader sees all three live", .timeLimit(.minutes(2)))
  func leaderElected() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkLeaderElected()
  }

  @available(anyAppleOS 27, *)
  private func checkLeaderElected() async throws {
    try await withGroup { members in
      let leader = try await ReplicationGroupFixture.leader(among: members)
      let deadline = ContinuousClock.now + .seconds(10)
      var live = await leader.server.liveMembers.map(\.id)
      while Set(live) != ["m1", "m2", "m3"], ContinuousClock.now < deadline {
        try await Task.sleep(for: .milliseconds(100))
        live = await leader.server.liveMembers.map(\.id)
      }
      #expect(Set(live) == ["m1", "m2", "m3"])
    }
  }

  @Test("a mutation sent to a follower commits and reaches a client on another member", .timeLimit(.minutes(2)))
  func mutationThroughFollower() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkMutationThroughFollower()
  }

  @available(anyAppleOS 27, *)
  private func checkMutationThroughFollower() async throws {
    try await withGroup { members in
      let leader = try await ReplicationGroupFixture.leader(among: members)
      let followers = members.filter { $0.id != leader.id }
      try await withClients(on: [followers[0], followers[1]]) { writer, reader in
        let query = try await reader.replica.subscribe(name: "note", arguments: .null)
        await reader.link.send()
        _ = try await reader.wait { if case .replica(.changed(let q, _)) = $0, q == query { true } else { false } }

        let mutation = try await writer.replica.mutate(name: "set-note", arguments: .text("through a follower"))
        await writer.link.send()
        let done = try await writer.wait { if case .replica(.done(let n, _)) = $0, n == mutation { true } else { false } }
        guard case .replica(.done(_, let outcome)) = done else { return }
        #expect(outcome == .value(.text("through a follower")))
        _ = try await reader.wait { if case .replica(.changed(let q, .text("through a follower"))) = $0, q == query { true } else { false } }
      }
      for member in members {
        try await expectNote("through a follower", in: member.database)
      }
    }
  }

  @Test("when the leader stops the others elect a new one and writes continue", .timeLimit(.minutes(3)))
  func leaderFailover() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkLeaderFailover()
  }

  @available(anyAppleOS 27, *)
  private func checkLeaderFailover() async throws {
    try await withGroup { members in
      let first = try await ReplicationGroupFixture.leader(among: members)
      first.stop()
      let survivors = members.filter { $0.id != first.id }
      let second = try await ReplicationGroupFixture.leader(among: survivors)
      #expect(second.id != first.id)

      let follower = try #require(survivors.first { $0.id != second.id })
      try await withClients(on: [follower, second]) { writer, reader in
        let query = try await reader.replica.subscribe(name: "note", arguments: .null)
        await reader.link.send()
        let mutation = try await writer.replica.mutate(name: "set-note", arguments: .text("after failover"))
        await writer.link.send()
        _ = try await writer.wait { if case .replica(.done(let n, .value)) = $0, n == mutation { true } else { false } }
        _ = try await reader.wait { if case .replica(.changed(let q, .text("after failover"))) = $0, q == query { true } else { false } }
      }
      for member in survivors { try await expectNote("after failover", in: member.database) }
    }
  }

  @Test("change on a follower throws notLeader", .timeLimit(.minutes(2)))
  func changeOnFollower() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkChangeOnFollower()
  }

  @available(anyAppleOS 27, *)
  private func checkChangeOnFollower() async throws {
    try await withGroup { members in
      let leader = try await ReplicationGroupFixture.leader(among: members)
      let follower = try #require(members.first { $0.id != leader.id })
      await #expect(throws: ReplicationError.notLeader(leader: leader.id)) {
        try await follower.server.change { _ in 1 }
      }
    }
  }

  @Test("a server-only mutation called on a follower commits on the leader and is readable at once", .timeLimit(.minutes(2)))
  func serverMutationOnFollower() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkServerMutationOnFollower()
  }

  @available(anyAppleOS 27, *)
  private func checkServerMutationOnFollower() async throws {
    try await withGroup { members in
      let leader = try await ReplicationGroupFixture.leader(among: members)
      let follower = try #require(members.first { $0.id != leader.id })
      let answer = try await follower.server.mutate("server-set-note", arguments: .text("by the worker"), as: .text("worker"))
      #expect(answer == .text("by the worker"))
      // No waiting: the call returned after the follower applied the entry.
      let stored = try await follower.database.collection(named: "notes").document(for: .text("main"))
      #expect(stored?["text"] == .text("by the worker"))
      try await expectNote("by the worker", in: leader.database)
      await #expect(throws: FunctionFailure.noSuchFunction("nothing")) {
        try await follower.server.mutate("nothing", as: .null)
      }
    }
  }

  @Test("a client cannot call a server-only mutation", .timeLimit(.minutes(2)))
  func clientCannotCallServerMutation() async throws {
    guard #available(anyAppleOS 27, *) else { return }
    try await checkClientCannotCallServerMutation()
  }

  @available(anyAppleOS 27, *)
  private func checkClientCannotCallServerMutation() async throws {
    try await withGroup { members in
      let leader = try await ReplicationGroupFixture.leader(among: members)
      let follower = try #require(members.first { $0.id != leader.id })
      try await withClients(on: [follower, leader]) { client, _ in
        // A client with the real functions refuses it before sending.
        await #expect(throws: FunctionFailure.noSuchFunction("server-set-note")) {
          try await client.replica.mutate(name: "server-set-note", arguments: .text("forbidden"))
        }
      }
      try await withClients(on: [follower, leader], functions: Self.tamperedClientFunctions) { client, _ in
        let mutation = try await client.replica.mutate(name: "server-set-note", arguments: .text("forbidden"))
        await client.link.send()
        let done = try await client.wait { if case .replica(.done(let n, _)) = $0, n == mutation { true } else { false } }
        guard case .replica(.done(_, let outcome)) = done else { return }
        #expect(outcome == .failure(FunctionFailure.noSuchFunction("server-set-note").localizedDescription))
      }
      let stored = try await leader.database.collection(named: "notes").document(for: .text("main"))
      #expect(stored == nil)
    }
  }

  // MARK: Fixtures

  /// Binds three members, serves them while `body` runs and stops them after.
  @available(anyAppleOS 27, *)
  private func withGroup(_ body: ([GroupMember]) async throws -> Void) async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-replication-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    let members = try await ReplicationGroupFixture.members(count: 3, root: root, functions: Self.functions)
    try await withThrowingTaskGroup(of: Void.self) { group in
      for member in members { group.addTask { await member.serve() } }
      defer { group.cancelAll() }
      try await body(members)
    }
  }

  /// Attaches a client to each member of `hosts` and runs `body` with them.
  @available(anyAppleOS 27, *)
  private func withClients(
    on hosts: [GroupMember],
    functions: Functions = Self.functions,
    _ body: (ReplicationClient, ReplicationClient) async throws -> Void
  ) async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-clients-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    let trust = try ReplicationGroupFixture.clientTrust()
    var clients: [ReplicationClient] = []
    for (index, host) in hosts.enumerated() {
      let directory = root.appending(path: "client-\(index)")
      try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
      clients.append(ReplicationClient(
        replica: try Replica(directory: directory, functions: functions), port: host.syncHost.port, device: "device-\(index)", trust: trust
      ))
    }
    try await withThrowingTaskGroup(of: Void.self) { group in
      for client in clients { group.addTask { await client.run() } }
      defer { group.cancelAll() }
      for client in clients {
        _ = try await client.wait { if case .replica(.welcomed) = $0 { true } else { false } }
      }
      try await body(clients[0], clients[1])
    }
  }

  /// Waits until the member's database holds the note.
  @available(anyAppleOS 27, *)
  private func expectNote(_ text: String, in database: Database) async throws {
    let deadline = ContinuousClock.now + .seconds(10)
    var stored: Value?
    while ContinuousClock.now < deadline {
      stored = try await database.collection(named: "notes").document(for: .text("main"))?["text"]
      if stored == .text(text) { break }
      try await Task.sleep(for: .milliseconds(100))
    }
    #expect(stored == .text(text))
  }
}

/// A sync client attached to one member's sync host.
@available(anyAppleOS 27, *)
struct ReplicationClient: Sendable {
  let replica: Replica
  let link: SyncLink
  let port: Int
  let device: String
  let trust: SyncTrust

  init(replica: Replica, port: Int, device: String, trust: SyncTrust) {
    self.replica = replica
    link = SyncLink(replica: replica)
    self.port = port
    self.device = device
    self.trust = trust
  }

  func run() async {
    await link.run(host: "localhost", port: port, credentials: .text(device), trust: trust)
  }

  /// The first event matching `predicate`, within twenty seconds.
  func wait(for predicate: @Sendable (SyncLinkEvent) -> Bool) async throws -> SyncLinkEvent {
    let deadline = ContinuousClock.now + .seconds(20)
    var iterator = link.events.makeAsyncIterator()
    while ContinuousClock.now < deadline, let event = await iterator.next(isolation: #isolation) {
      if predicate(event) { return event }
    }
    throw FixtureError.noLeader
  }
}
