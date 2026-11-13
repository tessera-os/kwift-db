import Foundation
import KwiftDB
import Logging
import NIOCore
import NIOPosix
import NIOQUIC

/// Carries one replication member's messages to the other members over QUIC
/// (spec section 9.9, application protocol `kiradb-replication`).
///
/// `bind` opens the UDP socket and starts the member's state; `run` serves until
/// its task is cancelled. While it runs the host:
///
/// - ticks the member's `ReplicationCoordinator` on a `ContinuousClock`, so
///   heartbeats and elections happen on time;
/// - dials every member whose id sorts after this member's, and accepts
///   connections from the others, so each pair of members shares exactly one
///   connection, and keeps redialing a member that is down;
/// - authenticates each connection by the group's shared secret;
/// - delivers each message the member sends to the connection of its addressee,
///   dropping it when there is none (the protocol tolerates lost messages), and
///   hands each message it receives to the member.
///
/// A host consumes its coordinator's `outgoing` stream, so one coordinator has at
/// most one host. The replicated `SyncServer` runs beside it:
///
/// ```swift
/// let host = try await ReplicationHost.bind(coordinator: coordinator, port: 4434,
///   certificate: certificate, trust: trust, secret: secret)
/// await withDiscardingTaskGroup { group in
///   group.addTask { await host.run() }
///   group.addTask { await server.run() }
///   group.addTask { await syncHost.run() }
/// }
/// ```
@available(anyAppleOS 27, *)
public actor ReplicationHost {
  private typealias Multiplexer = QUICHandler<QUICStreamChannels>.ConnectionMultiplexer<SyncStreamChannel>

  /// How often the host ticks the member.
  static let tickInterval: Duration = .milliseconds(50)
  /// How long the host waits before it dials a member again.
  static let redialInterval: Duration = .milliseconds(500)

  /// The UDP port the host listens on.
  public nonisolated let port: Int

  let coordinator: ReplicationCoordinator
  let secret: ReplicationSecret
  let trust: SyncTrust
  let logger: Logger
  private let channel: any Channel
  private let multiplexer: Multiplexer
  private var links: [UUID: (peer: String, sink: AsyncStream<ReplicationMessage>.Continuation)] = [:]

  private init(
    coordinator: ReplicationCoordinator, secret: ReplicationSecret, trust: SyncTrust, logger: Logger,
    channel: any Channel, multiplexer: Multiplexer, port: Int
  ) {
    self.coordinator = coordinator
    self.secret = secret
    self.trust = trust
    self.logger = logger
    self.channel = channel
    self.multiplexer = multiplexer
    self.port = port
  }

  /// Starts the member and binds the host's UDP socket. Nothing is served
  /// until `run`.
  ///
  /// - Parameters:
  ///   - coordinator: The member to carry.
  ///   - address: The local address to bind; `::` takes every IPv6 and IPv4
  ///     address on dual-stack systems.
  ///   - port: The UDP port; 0 lets the system choose (read it from `port`).
  ///   - certificate: The certificate presented to the members that dial this one.
  ///   - trust: Which certificates the members this one dials may present.
  ///   - secret: The group's shared secret.
  ///   - logger: Where the host and SwiftNIO QUIC log.
  /// - Throws: `SyncTransportError.listener` when the socket cannot be bound or
  ///   the certificate cannot be loaded; the member's own errors when it cannot
  ///   recover its state.
  public static func bind(
    coordinator: ReplicationCoordinator,
    address: String = "::",
    port: Int,
    certificate: SyncHostCertificate,
    trust: SyncTrust,
    secret: ReplicationSecret,
    logger: Logger = Logger(label: "kwiftdb.replication")
  ) async throws -> ReplicationHost {
    try await coordinator.start()
    let configuration = certificate.configuration(applicationProtocol: ReplicationTLS.applicationProtocol)
    do {
      let (channel, multiplexer) = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .bind(host: address, port: port) { channel in
          channel.eventLoop.makeCompletedFuture {
            let (handler, multiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
              channel: channel,
              quicConfiguration: configuration,
              logger: logger,
              inboundStreamChannelInitializer: { SyncStreamChannel.wrapping($0) }
            )
            try channel.pipeline.syncOperations.addHandler(handler)
            return (channel, multiplexer)
          }
        }
      guard let bound = channel.localAddress?.port else {
        try? await channel.close()
        throw SyncTransportError.listener("the socket has no local port")
      }
      return ReplicationHost(
        coordinator: coordinator, secret: secret, trust: trust, logger: logger,
        channel: channel, multiplexer: multiplexer, port: bound
      )
    } catch let error as SyncTransportError {
      throw error
    } catch {
      throw SyncTransportError.listener(String(describing: error))
    }
  }

  /// Serves the member until the calling task is cancelled.
  ///
  /// Cancellation closes every connection with code 0 and then the socket.
  public nonisolated func run() async {
    await withDiscardingTaskGroup { group in
      group.addTask { await self.accept() }
      group.addTask { await self.dialMembers() }
      group.addTask { await self.relayOutgoing() }
      group.addTask { await self.tick() }
    }
    try? await channel.close()
  }

  // MARK: Links

  /// Makes `sink` the way to reach `peer`, ending any older link to it.
  func register(_ peer: String, _ sink: AsyncStream<ReplicationMessage>.Continuation) -> UUID {
    for (id, link) in links where link.peer == peer {
      link.sink.finish()
      links.removeValue(forKey: id)
    }
    let id = UUID()
    links[id] = (peer, sink)
    return id
  }

  func unregister(_ peer: String, link: UUID) {
    links.removeValue(forKey: link)
  }

  private func send(_ message: PeerMessage) {
    links.values.first { $0.peer == message.member }?.sink.yield(message.message)
  }

  private nonisolated func relayOutgoing() async {
    for await message in coordinator.outgoing { await send(message) }
  }

  // MARK: Time

  private nonisolated func tick() async {
    let clock = ContinuousClock()
    while !Task.isCancelled {
      do { try await coordinator.tick() } catch { logger.error("replication tick failed: \(error)") }
      try? await clock.sleep(for: Self.tickInterval)
    }
  }

  // MARK: Accepting

  private nonisolated func accept() async {
    await withDiscardingTaskGroup { group in
      for await connection in multiplexer.inboundConnections {
        group.addTask { await self.serve(connection) }
      }
    }
  }

  /// Runs a connection's conversation on its first stream and stops the others.
  private nonisolated func serve(_ connection: QUICConnection<SyncStreamChannel>) async {
    await withDiscardingTaskGroup { group in
      var adopted = false
      for await stream in connection.inboundStreams {
        if adopted {
          stream.channel.stopUnusedStream()
        } else {
          adopted = true
          group.addTask { await self.converse(on: stream, expecting: nil) }
        }
      }
    }
  }

  /// Runs a peer session on `stream` and closes its connection with the outcome.
  nonisolated func converse(on stream: SyncStreamChannel, expecting member: String?) async {
    let session = ReplicationPeerSession(coordinator: coordinator, host: self, secret: secret, expecting: member)
    let close: SyncClose
    do {
      close = try await stream.executeThenClose { inbound, outbound in
        await session.run(inbound: inbound, outbound: outbound)
      }
    } catch {
      close = .failure(String(describing: error))
    }
    if close.code != 0 { logger.info("replication connection ended: \(close.reason ?? "")") }
    stream.channel.closeConnection(close)
  }
}
