import Foundation
import KwiftDB
import Testing

/// What a replica keeps of rows the server releases (`ReplicaRetention`).
@Suite("Replica retention")
struct ReplicaRetentionTests {
  @Test("keepReleased keeps released rows readable offline and after reopening, and a new hold replaces them", .timeLimit(.minutes(1)))
  func keepReleased() async throws {
    let root = try Self.directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let link = try await InProcessLink(root: root, retention: .keepReleased)

    try await link.server.set("first")
    let query = try await link.subscribeAndSync()
    #expect(await link.replica.value(of: query) == .text("first"))
    try await link.unsubscribeAndSync(query)

    // The server changes the row while the replica does not hold it.
    try await link.server.set("second")

    // Before the server answers, a new subscription reads the released rows.
    let cached = try await link.replica.subscribe(name: "note", arguments: .null)
    #expect(await link.replica.value(of: cached) == .text("first"))
    #expect(await link.replica.isReady(cached) == false)

    // Held again, the range holds exactly the server's rows.
    try await link.sync()
    #expect(await link.replica.isReady(cached))
    #expect(await link.replica.value(of: cached) == .text("second"))

    // Closed and opened again offline, the replica still reads them.
    try await link.replica.close()
    let reopened = try Replica(directory: root.appending(path: "replica"), functions: InProcessLink.functions, retention: .keepReleased)
    let cold = try await reopened.subscribe(name: "note", arguments: .null)
    #expect(await reopened.value(of: cold) == .text("second"))
    #expect(await reopened.isReady(cold) == false)
    try await reopened.close()
  }

  @Test("exact removes released rows", .timeLimit(.minutes(1)))
  func exact() async throws {
    let root = try Self.directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let link = try await InProcessLink(root: root, retention: .exact)
    try await link.server.set("first")
    let query = try await link.subscribeAndSync()
    try await link.unsubscribeAndSync(query)
    let probe = try await link.replica.subscribe(name: "note", arguments: .null)
    #expect(await link.replica.value(of: probe) == .null)
  }

  @Test("keepLingering takes a let-go query back ready, and ends the oldest past the limit", .timeLimit(.minutes(1)))
  func keepLingering() async throws {
    let root = try Self.directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let link = try await InProcessLink(root: root, retention: .exact)
    try await link.replica.keepLingering(1)
    try await link.server.set("first")

    let query = try await link.subscribeAndSync()
    try await link.unsubscribeAndSync(query)
    let again = try await link.replica.subscribe(name: "note", arguments: .null)
    #expect(again == query)
    #expect(await link.replica.isReady(again))
    #expect(await link.replica.value(of: again) == .text("first"))
    #expect(await link.replica.takeOutgoing().isEmpty)

    // A second let-go query pushes the first out for real.
    let other = try await link.replica.subscribe(name: "note", arguments: .text("other"))
    try await link.replica.unsubscribe(again)
    try await link.replica.unsubscribe(other)
    let sent = await link.replica.takeOutgoing()
    #expect(sent.contains { if case .unsubscribe(let ended) = $0 { ended == query } else { false } })
    #expect(!sent.contains { if case .unsubscribe(let ended) = $0 { ended == other } else { false } })
  }

  private static func directory() throws -> URL {
    let root = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-retention-\(UUID().uuidString)")
    for folder in ["server", "replica"] {
      try FileManager.default.createDirectory(at: root.appending(path: folder), withIntermediateDirectories: true)
    }
    return root
  }
}

/// A server and one replica joined in process.
private struct InProcessLink {
  static let functions: Functions = try! Functions(
    queries: [
      QueryFunction(name: "note") { run, _, _ in
        try await run.collection(named: "notes").document(for: .text("main"))?["text"] ?? .null
      }
    ],
    mutations: []
  )

  let server: SyncServer
  let replica: Replica
  let connection: UUID

  init(root: URL, retention: ReplicaRetention) async throws {
    server = SyncServer(database: try Database(directory: root.appending(path: "server")), functions: Self.functions) { _ in .text("owner") }
    replica = try Replica(directory: root.appending(path: "replica"), functions: Self.functions, retention: retention)
    let hello = try await replica.connect(credentials: .null)
    guard case .hello(let client, let credentials)? = hello.first,
          case .accepted(let connection, let welcome) = await server.connect(client: client, credentials: credentials)
    else { throw FunctionFailure.refused("not accepted") }
    self.connection = connection
    _ = try await replica.receive(welcome)
  }

  func subscribeAndSync() async throws -> Number {
    let query = try await replica.subscribe(name: "note", arguments: .null)
    try await sync()
    return query
  }

  func unsubscribeAndSync(_ query: Number) async throws {
    try await replica.unsubscribe(query)
    try await sync()
  }

  /// Delivers the replica's queued messages and the server's replies.
  func sync() async throws {
    for message in await replica.takeOutgoing() {
      for reply in try await server.receive(message, from: connection) { _ = try await replica.receive(reply) }
    }
  }
}

extension SyncServer {
  /// Writes the note as server code.
  fileprivate func set(_ text: String) async throws {
    try await change { run in
      try await run.collection(named: "notes").put(try Document(fields: [DocumentField("text", .text(text))]), for: .text("main"))
    }
  }
}
