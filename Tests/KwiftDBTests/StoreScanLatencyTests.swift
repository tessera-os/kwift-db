import Foundation
import KwiftDB
import Testing

/// Reading one conversation's rows out of a store that holds thousands of
/// others costs milliseconds: a range scan reads only the blocks it covers and
/// decodes each once.
@Suite("Store scan latency")
struct StoreScanLatencyTests {
  @Test("a 200-row range out of 20,000 scans in under 20 ms")
  func rangeScanIsFast() async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "store-scan-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    let store = try await Self.filledStore(at: root)
    let clock = ContinuousClock()
    var slowest = Duration.zero
    for conversation in stride(from: 0, to: 100, by: 7) {
      let started = clock.now
      let rows = try await store.scan(
        from: Self.key(conversation: conversation, message: 0),
        to: Self.key(conversation: conversation, message: 1_000))
      slowest = max(slowest, clock.now - started)
      #expect(rows.count == 200)
    }
    print("store scan latency: slowest \(slowest)")
    #expect(slowest < .milliseconds(20), "slowest scan took \(slowest)")
    try await store.close()
  }

  @Test("reading a conversation's 200 rows one key at a time takes under 20 ms")
  func pointReadsAreFast() async throws {
    let root = FileManager.default.temporaryDirectory.appending(path: "store-points-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: root) }
    let store = try await Self.filledStore(at: root)
    let clock = ContinuousClock()
    var slowest = Duration.zero
    for conversation in stride(from: 0, to: 100, by: 7) {
      let started = clock.now
      var found = 0
      // A query reads each row by key; a replica then restates each as a
      // one-key range.
      for message in 0..<200 {
        let key = Self.key(conversation: conversation, message: message)
        if try await store.get(key) != nil { found += 1 }
        found += try await store.scan(from: key, to: key + [0]).count
      }
      slowest = max(slowest, clock.now - started)
      #expect(found == 400)
    }
    print("store point read latency: slowest conversation \(slowest)")
    #expect(slowest < .milliseconds(20), "slowest conversation took \(slowest)")
    try await store.close()
  }

  /// 100 conversations of 200 messages, 3 KB each, written interleaved the
  /// way replies arrive, so every conversation spans many tables.
  private static func filledStore(at root: URL) async throws -> Store {
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
    let store = try Store(directory: root)
    let value = [UInt8](repeating: 9, count: 3_000)
    for message in 0..<200 {
      var batch = WriteBatch()
      for conversation in 0..<100 {
        batch.put(value, for: key(conversation: conversation, message: message))
      }
      try await store.write(batch)
    }
    return store
  }

  private static func key(conversation: Int, message: Int) -> [UInt8] {
    Array("m-\(String(format: "%04d", conversation))-\(String(format: "%04d", message))".utf8)
  }
}
