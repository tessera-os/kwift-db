import Foundation
import KwiftDB
import KwiftDBQUIC
import Testing

/// One member of an in-process replication group on loopback: its database, its
/// replicated sync server and both QUIC hosts, each on its own directory and port.
@available(anyAppleOS 27, *)
struct GroupMember: Sendable {
  let id: String
  let database: Database
  let coordinator: ReplicationCoordinator
  let server: SyncServer
  let replicationHost: ReplicationHost
  let syncHost: SyncHost
  private let stopSignal: AsyncStream<Void>
  private let stopper: AsyncStream<Void>.Continuation

  fileprivate init(
    id: String, database: Database, coordinator: ReplicationCoordinator, server: SyncServer,
    replicationHost: ReplicationHost, syncHost: SyncHost
  ) {
    self.id = id
    self.database = database
    self.coordinator = coordinator
    self.server = server
    self.replicationHost = replicationHost
    self.syncHost = syncHost
    (stopSignal, stopper) = AsyncStream.makeStream(of: Void.self)
  }

  /// Serves the member until `stop()` is called or the calling task is cancelled.
  func serve() async {
    await withTaskGroup(of: Bool.self) { group in
      group.addTask { await replicationHost.run(); return false }
      group.addTask { await server.run(); return false }
      group.addTask { await syncHost.run(); return false }
      group.addTask {
        for await _ in stopSignal {}
        return true
      }
      for await stopped in group where stopped { group.cancelAll() }
    }
  }

  /// Ends `serve()`, as if the member's process stopped.
  func stop() { stopper.finish() }
}

/// Builds replication groups on loopback with the fixtures' certificates.
@available(anyAppleOS 27, *)
enum ReplicationGroupFixture {
  static let secret = ReplicationSecret("fixture secret")

  /// Creates `count` members named `m1`, `m2`, ... under `root`, bound and not yet serving.
  static func members(
    count: Int, root: URL, functions: Functions
  ) async throws -> [GroupMember] {
    let fixtures = try #require(Bundle.module.url(forResource: "Fixtures", withExtension: nil))
    let certificate = SyncHostCertificate(
      serverName: "localhost",
      chainPath: fixtures.appending(path: "loopback-chain.pem").path,
      privateKeyPath: fixtures.appending(path: "loopback-key.pem").path
    )
    let trust = SyncTrust.roots(path: fixtures.appending(path: "loopback-root.pem").path)
    let ports = try (0..<count).map { _ in try reserveUDPPort() }
    let list = (0..<count).map { Member(id: "m\($0 + 1)", address: "localhost:\(ports[$0])") }

    var members: [GroupMember] = []
    for (index, member) in list.enumerated() {
      let directory = root.appending(path: member.id)
      let replication = directory.appending(path: "replication")
      try FileManager.default.createDirectory(at: replication, withIntermediateDirectories: true)
      let database = try Database(directory: directory.appending(path: "data"))
      let coordinator = try ReplicationCoordinator(
        group: try Group(members: list, me: member.id), database: database, replicationDirectory: replication
      )
      let server = SyncServer(database: database, functions: functions, replication: coordinator) { credentials in
        guard case .text(let device) = credentials, !device.isEmpty else { throw FunctionFailure.refused("device required") }
        return .text(device)
      }
      let replicationHost = try await ReplicationHost.bind(
        coordinator: coordinator, port: ports[index], certificate: certificate, trust: trust, secret: secret
      )
      let syncHost = try await SyncHost.bind(server: server, port: 0, certificate: certificate)
      members.append(GroupMember(
        id: member.id, database: database, coordinator: coordinator, server: server,
        replicationHost: replicationHost, syncHost: syncHost
      ))
    }
    return members
  }

  /// The trust a sync client needs for the fixtures' certificates.
  static func clientTrust() throws -> SyncTrust {
    let fixtures = try #require(Bundle.module.url(forResource: "Fixtures", withExtension: nil))
    return .roots(path: fixtures.appending(path: "loopback-root.pem").path)
  }

  /// Polls until exactly one of `candidates` is the leader and every one of them
  /// agrees, or thirty seconds pass.
  static func leader(among candidates: [GroupMember]) async throws -> GroupMember {
    let deadline = ContinuousClock.now + .seconds(30)
    while ContinuousClock.now < deadline {
      var leaders: [GroupMember] = []
      for member in candidates where await member.coordinator.status().role == .leader { leaders.append(member) }
      if let leader = leaders.first, leaders.count == 1 {
        var agreed = true
        for member in candidates where await member.coordinator.status().leader != leader.id { agreed = false }
        if agreed { return leader }
      }
      try await Task.sleep(for: .milliseconds(100))
    }
    throw FixtureError.noLeader
  }

  /// Finds a free UDP port by binding one and letting it go.
  private static func reserveUDPPort() throws -> Int {
    let descriptor = socket(AF_INET6, SOCK_DGRAM, 0)
    guard descriptor >= 0 else { throw FixtureError.noPort }
    defer { close(descriptor) }
    var address = sockaddr_in6()
    address.sin6_len = UInt8(MemoryLayout<sockaddr_in6>.size)
    address.sin6_family = sa_family_t(AF_INET6)
    address.sin6_addr = in6addr_any
    let bound = withUnsafePointer(to: &address) {
      $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in6>.size)) }
    }
    var length = socklen_t(MemoryLayout<sockaddr_in6>.size)
    let named = withUnsafeMutablePointer(to: &address) {
      $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(descriptor, $0, &length) }
    }
    guard bound == 0, named == 0 else { throw FixtureError.noPort }
    return Int(UInt16(bigEndian: address.sin6_port))
  }
}

enum FixtureError: Error {
  case noLeader
  case noPort
}
