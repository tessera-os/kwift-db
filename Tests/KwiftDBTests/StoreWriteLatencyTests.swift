import Foundation
import KwiftDB
import Testing

/// A write's cost tracks what it writes, not how much the store holds: the
/// flushes and compactions that keep the store in shape must not stall one
/// writer for seconds.
@Suite("Store write latency")
struct StoreWriteLatencyTests {
  @Test("no single write stalls while 40 MB go through the store", .timeLimit(.minutes(2)))
  func writesStayFast() async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "store-latency-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
    let store = try Store(directory: root)
    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask { try await store.maintain() }
      try await writeRows(to: store)
      group.cancelAll()
    }
    // Everything written reads back after upkeep.
    #expect(try await store.get(Array("row-0-0-0".utf8)) != nil)
    try await store.close()
    let reopened = try Store(directory: root)
    #expect(try await reopened.scan(from: Array("row-".utf8), to: Array("row.".utf8)).count == 20_000)
    try await reopened.close()
  }

  private func writeRows(to store: Store) async throws {
    let clock = ContinuousClock()
    let value = [UInt8](repeating: 7, count: 2_000)
    var slowest = Duration.zero
    var total = Duration.zero
    // 20,000 rows of 2 KB in batches of 10, keys spread over the key space
    // the way ids are.
    for batchNumber in 0..<2_000 {
      var batch = WriteBatch()
      for row in 0..<10 {
        let id = (batchNumber * 10 + row) &* 2_654_435_761 % 1_000_003
        batch.put(value, for: Array("row-\(id)-\(batchNumber)-\(row)".utf8))
      }
      let started = clock.now
      try await store.write(batch)
      let took = clock.now - started
      total += took
      slowest = max(slowest, took)
    }
    print("store write latency: total \(total), slowest \(slowest)")
    #expect(slowest < .milliseconds(100), "slowest write took \(slowest)")
  }
}
