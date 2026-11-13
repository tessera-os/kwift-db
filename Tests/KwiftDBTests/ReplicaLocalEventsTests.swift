import Foundation
import KwiftDB
import Testing

/// What a replica shows before and around the server: local values at once,
/// speculative mutations at once, and when a query's server result is in.
@Suite("Replica local events")
struct ReplicaLocalEventsTests {
  static let functions: Functions = try! Functions(
    queries: [
      QueryFunction(name: "note") { run, _, _ in
        try await run.collection(named: "notes").document(for: .text("main"))?["text"] ?? .null
      }
    ],
    mutations: [
      MutationFunction(name: "set-note") { run, _, arguments in
        try await run.collection(named: "notes").put(try Document(fields: [DocumentField("text", arguments)]), for: .text("main"))
        return arguments
      }
    ]
  )

  @Test("offline, a subscription's value and a mutation's effect are events at once")
  func offlineEvents() async throws {
    let root = try Self.directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let replica = try Replica(directory: root, functions: Self.functions)

    let query = try await replica.subscribe(name: "note", arguments: .null)
    #expect(Self.changes(in: await replica.takeEvents(), for: query) == [.null])
    #expect(await replica.value(of: query) == .null)
    #expect(await replica.isReady(query) == false)

    _ = try await replica.mutate(name: "set-note", arguments: .text("offline"))
    #expect(Self.changes(in: await replica.takeEvents(), for: query) == [.text("offline")])
    #expect(await replica.value(of: query) == .text("offline"))
    #expect(await replica.takeEvents().isEmpty)
  }

  @Test("a query becomes ready when the server sends its first result")
  func readyEvent() async throws {
    let root = try Self.directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let server = SyncServer(database: try Database(directory: root.appending(path: "server")), functions: Self.functions) { _ in .text("owner") }
    let replica = try Replica(directory: root.appending(path: "replica"), functions: Self.functions)

    let hello = try await replica.connect(credentials: .null)
    guard case .hello(let client, let credentials)? = hello.first,
          case .accepted(let connection, let welcome) = await server.connect(client: client, credentials: credentials)
    else { Issue.record("not accepted"); return }
    _ = try await replica.receive(welcome)

    let query = try await replica.subscribe(name: "note", arguments: .null)
    var events: [ReplicaEvent] = []
    for message in await replica.takeOutgoing() {
      for reply in try await server.receive(message, from: connection) { events += try await replica.receive(reply) }
    }
    #expect(events.contains { if case .ready(query) = $0 { true } else { false } })
    #expect(await replica.isReady(query))
  }

  private static func changes(in events: [ReplicaEvent], for query: Number) -> [Value] {
    events.compactMap { if case .changed(query, let value) = $0 { value } else { nil } }
  }

  private static func directory() throws -> URL {
    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-local-\(UUID().uuidString)")
    for folder in ["server", "replica"] {
      try FileManager.default.createDirectory(at: root.appending(path: folder), withIntermediateDirectories: true)
    }
    return root
  }
}
