import Foundation
import KwiftDB
import Testing

/// Two stores on one directory corrupt it, so a replica opened with
/// `Replica.open` waits for the one before it to close.
@Suite("Replica directory claim")
struct ReplicaDirectoryClaimTests {
  actor Order {
    private(set) var steps: [String] = []
    func record(_ step: String) { steps.append(step) }
  }

  @Test("a second open waits for the first replica to close", .timeLimit(.minutes(1)))
  func secondOpenWaits() async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "replica-claim-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    let functions = try Functions(queries: [], mutations: [])
    let first = try await Replica.open(directory: root, functions: functions)
    let order = Order()
    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask {
        let second = try await Replica.open(directory: root, functions: functions)
        await order.record("second opened")
        try await second.close()
      }
      try await Task.sleep(for: .milliseconds(100))
      await order.record("first closing")
      try await first.close()
      try await group.waitForAll()
    }
    #expect(await order.steps == ["first closing", "second opened"])
  }
}
