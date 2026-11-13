import Foundation
import KwiftDB
import Testing

/// A server whose record of a client went back, as after a restore, refuses
/// the client's pending mutations as out of order. The replica renumbers them
/// after the server's applied number, so the next session applies them.
@Suite("Replica renumbering")
struct ReplicaRenumberTests {
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

  @Test("pending mutations refused as out of order are renumbered and then applied")
  func renumbersAfterRestore() async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-renumber-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    for folder in ["first", "restored", "replica"] {
      try FileManager.default.createDirectory(at: root.appending(path: folder), withIntermediateDirectories: true)
    }
    let first = SyncServer(database: try Database(directory: root.appending(path: "first")), functions: Self.functions) { _ in .text("owner") }
    let restored = SyncServer(database: try Database(directory: root.appending(path: "restored")), functions: Self.functions) { _ in .text("owner") }
    let replica = try Replica(directory: root.appending(path: "replica"), functions: Self.functions)
    _ = try await replica.subscribe(name: "note", arguments: .null)

    _ = try await replica.mutate(name: "set-note", arguments: .text("one"))
    _ = try await replica.mutate(name: "set-note", arguments: .text("two"))
    _ = try await Self.session(replica, first)
    let pending = try await replica.mutate(name: "set-note", arguments: .text("three"))
    #expect(pending == Number(3))

    let refused = try await Self.session(replica, restored)
    #expect(refused.contains { if case .refused("mutation out of order") = $0 { true } else { false } })
    #expect(refused.contains { if case .renumbered(Number(3), Number(1)) = $0 { true } else { false } })

    let applied = try await Self.session(replica, restored)
    #expect(applied.contains { if case .done(Number(1), .value(.text("three"))) = $0 { true } else { false } })
    #expect(try await replica.mutate(name: "set-note", arguments: .text("four")) == Number(2))
  }

  /// Runs one session until the server has answered everything or closed it.
  private static func session(_ replica: Replica, _ server: SyncServer) async throws -> [ReplicaEvent] {
    let messages = try await replica.connect(credentials: .null)
    guard case .hello(let client, let credentials)? = messages.first,
          case .accepted(let connection, let welcome) = await server.connect(client: client, credentials: credentials)
    else { Issue.record("not accepted"); return [] }
    var events = try await replica.receive(welcome)
    for message in messages.dropFirst() {
      guard let replies = try? await server.receive(message, from: connection) else { break }
      for reply in replies { events += try await replica.receive(reply) }
    }
    try await replica.disconnect()
    return events
  }
}
