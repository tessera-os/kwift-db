import Foundation
import KwiftDB
import Testing

/// A replica re-runs only the queries whose reads a change touches, so a
/// burst of server transitions costs what it changes, not every subscription.
@Suite("Replica refresh")
struct ReplicaRefreshTests {
  /// Counts how often each named note's query ran.
  actor Runs {
    private(set) var counts: [String: Int] = [:]
    func record(_ name: String) { counts[name, default: 0] += 1 }
  }

  static func functions(counting runs: Runs) throws -> Functions {
    try Functions(
      queries: [
        QueryFunction(name: "note") { run, _, arguments in
          guard case .text(let name) = arguments else { return .null }
          await runs.record(name)
          return try await run.collection(named: "notes").document(for: .text(name))?["text"] ?? .null
        }
      ],
      mutations: [
        MutationFunction(name: "set-note") { run, _, arguments in
          guard case .document(let document) = arguments, case .text(let name)? = document["name"], let text = document["text"]
          else { return .null }
          try await run.collection(named: "notes").put(try Document(fields: [DocumentField("text", text)]), for: .text(name))
          return text
        },
        MutationFunction(name: "hand-off", speculative: false) { run, _, arguments in
          try await run.collection(named: "notes").put(try Document(fields: [DocumentField("text", arguments)]), for: .text("a"))
          return .null
        },
        MutationFunction(name: "log") { run, _, arguments in
          try await run.collection(named: "log").put(try Document(fields: [DocumentField("line", arguments)]), for: arguments)
          return .null
        },
      ]
    )
  }

  @Test("a server write re-runs only the query that read it")
  func serverWriteRerunsOnlyItsQuery() async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "replica-refresh-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    let runs = Runs()
    let functions = try Self.functions(counting: runs)
    let server = SyncServer(database: try Database(directory: root.appending(path: "server")), functions: functions) { _ in .text("owner") }
    let replica = try Replica(directory: root.appending(path: "replica"), functions: functions)
    let hello = try await replica.connect(credentials: .null)
    guard case .hello(let client, let credentials)? = hello.first,
          case .accepted(let connection, let welcome) = await server.connect(client: client, credentials: credentials)
    else { Issue.record("not accepted"); return }
    _ = try await replica.receive(welcome)

    func sync() async throws -> [ReplicaEvent] {
      var events: [ReplicaEvent] = []
      for message in await replica.takeOutgoing() {
        for reply in try await server.receive(message, from: connection) { events += try await replica.receive(reply) }
      }
      return events
    }

    let first = try await replica.subscribe(name: "note", arguments: .text("a"))
    let second = try await replica.subscribe(name: "note", arguments: .text("b"))
    _ = try await sync()
    let before = await runs.counts

    let note = Value.document(try Document(fields: [DocumentField("name", .text("a")), DocumentField("text", .text("hello"))]))
    _ = try await replica.mutate(name: "set-note", arguments: note)
    _ = try await sync()
    #expect(await replica.value(of: first) == .text("hello"))
    #expect(await replica.value(of: second) == .null)
    let after = await runs.counts
    #expect(after["b"] == before["b"], "the query of note b read nothing that changed")
    #expect((after["a"] ?? 0) > (before["a"] ?? 0))

    // A pending mutation no query reads leaves every query alone.
    let settled = await runs.counts
    _ = try await replica.mutate(name: "log", arguments: .text("line-1"))
    #expect(await runs.counts == settled)

    // A mutation that is not speculative is queued but not run locally, even
    // when it writes rows a query reads.
    _ = try await replica.mutate(name: "hand-off", arguments: .text("for the server"))
    #expect(await runs.counts == settled)
    #expect(await replica.value(of: first) == .text("hello"))
    #expect(await replica.takeOutgoing().contains { if case .mutate(_, "hand-off", _) = $0 { true } else { false } })
  }

  @Test("resubscribing after a reconnect re-runs no query when the rows did not change")
  func reconnectWithUnchangedRowsRerunsNothing() async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "replica-refresh-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    // The server runs the query to answer each subscription; only the
    // replica's own runs are counted.
    let runs = Runs()
    let server = SyncServer(
      database: try Database(directory: root.appending(path: "server")), functions: try Self.functions(counting: Runs())
    ) { _ in .text("owner") }
    let replica = try Replica(directory: root.appending(path: "replica"), functions: try Self.functions(counting: runs))

    func session() async throws -> UUID? {
      let messages = try await replica.connect(credentials: .null)
      guard case .hello(let client, let credentials)? = messages.first,
            case .accepted(let connection, let welcome) = await server.connect(client: client, credentials: credentials)
      else { return nil }
      _ = try await replica.receive(welcome)
      for message in messages.dropFirst() {
        for reply in try await server.receive(message, from: connection) { _ = try await replica.receive(reply) }
      }
      return connection
    }
    func sync(_ connection: UUID) async throws {
      for message in await replica.takeOutgoing() {
        for reply in try await server.receive(message, from: connection) { _ = try await replica.receive(reply) }
      }
    }

    guard let first = try await session() else { Issue.record("not accepted"); return }
    let query = try await replica.subscribe(name: "note", arguments: .text("a"))
    let note = Value.document(try Document(fields: [DocumentField("name", .text("a")), DocumentField("text", .text("hello"))]))
    _ = try await replica.mutate(name: "set-note", arguments: note)
    try await sync(first)
    #expect(await replica.value(of: query) == .text("hello"))

    try await replica.disconnect()
    await server.disconnect(first)
    let settled = await runs.counts
    guard try await session() != nil else { Issue.record("not accepted again"); return }
    #expect(await replica.isReady(query))
    #expect(await replica.value(of: query) == .text("hello"))
    let after = await runs.counts
    #expect(after == settled, "the held rows came back unchanged: \(settled) -> \(after)")
  }
}
