import Foundation
import KwiftDB
import Testing

@Suite("Replica mutation access")
struct ReplicaAccessTests {
  @Test("a replica refuses a server-only mutation without queueing it")
  func serverOnlyRefused() async throws {
    let directory = FileManager.default.temporaryDirectory.appending(path: "kwiftdb-access-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let functions = try Functions(
      queries: [],
      mutations: [MutationFunction(name: "server-only", access: .server) { _, _, arguments in arguments }]
    )
    let replica = try Replica(directory: directory, functions: functions)
    await #expect(throws: FunctionFailure.noSuchFunction("server-only")) {
      try await replica.mutate(name: "server-only", arguments: .null)
    }
    #expect(await replica.takeOutgoing().isEmpty)
  }
}
