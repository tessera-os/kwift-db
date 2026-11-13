import Foundation
import KwiftDB
import Testing

/// Server-owned changes (`SyncServer.change`) reach subscribed replicas
/// through the same transitions as client mutations.
@Suite("Server-owned changes")
struct ServerChangeTests {
  static let functions: Functions = try! Functions(
    queries: [
      QueryFunction(name: "note") { run, _, _ in
        try await run.collection(named: "notes").document(for: .text("main"))?["text"] ?? .null
      }
    ],
    mutations: []
  )

  struct Refusal: Error, Equatable {}

  @Test("a change re-runs the subscriptions that read what it wrote")
  func changeReachesReplica() async throws {
    let root = try Self.directories()
    defer { try? FileManager.default.removeItem(at: root) }
    let database = try Database(directory: root.appending(path: "server"))
    let server = SyncServer(database: database, functions: Self.functions) { _ in .text("owner") }
    let replica = try Replica(directory: root.appending(path: "replica"), functions: Self.functions)

    let connection = try await Self.connect(replica, to: server)
    let query = try await replica.subscribe(name: "note", arguments: .null)
    try await Self.deliver(replica.takeOutgoing(), from: replica, to: server, connection: connection)

    var commits = server.commits.makeAsyncIterator()
    let answer = try await server.change { run in
      try await run.collection(named: "notes").put(
        try Document(fields: [DocumentField("text", .text("from the server"))]),
        for: .text("main")
      )
      return "done"
    }
    #expect(answer == "done")
    #expect(await commits.next() == (try await database.version()))

    var changed: Value?
    for message in await server.drain(connection) {
      for event in try await replica.receive(message) {
        if case .changed(let number, let value) = event, number == query { changed = value }
      }
    }
    #expect(changed == .text("from the server"))
  }

  @Test("a change that throws writes nothing and rethrows its error")
  func failedChangeWritesNothing() async throws {
    let root = try Self.directories()
    defer { try? FileManager.default.removeItem(at: root) }
    let database = try Database(directory: root.appending(path: "server"))
    let server = SyncServer(database: database, functions: Self.functions) { _ in .text("owner") }
    let before = try await database.version()

    await #expect(throws: Refusal.self) {
      try await server.change { run in
        try await run.collection(named: "notes").put(
          try Document(fields: [DocumentField("text", .text("never"))]),
          for: .text("main")
        )
        throw Refusal()
      }
    }
    #expect(try await database.version() == before)
    #expect(try await database.collection(named: "notes").document(for: .text("main")) == nil)
  }

  private static func directories() throws -> URL {
    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-change-\(UUID().uuidString)")
    for folder in ["server", "replica"] {
      try FileManager.default.createDirectory(at: root.appending(path: folder), withIntermediateDirectories: true)
    }
    return root
  }

  /// Opens a session in process, without a transport.
  private static func connect(_ replica: Replica, to server: SyncServer) async throws -> UUID {
    let hello = try await replica.connect(credentials: .null)
    guard case .hello(let client, let credentials)? = hello.first,
          case .accepted(let connection, let welcome) = await server.connect(client: client, credentials: credentials)
    else { throw Refusal() }
    _ = try await replica.receive(welcome)
    return connection
  }

  private static func deliver(_ messages: [SyncMessage], from replica: Replica, to server: SyncServer, connection: UUID) async throws {
    for message in messages {
      for reply in try await server.receive(message, from: connection) { _ = try await replica.receive(reply) }
    }
  }
}
