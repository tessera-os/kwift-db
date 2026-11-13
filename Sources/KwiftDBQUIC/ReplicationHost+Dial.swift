import Foundation
import KwiftDB
import Logging
import NIOCore
import NIOPosix
import NIOQUIC
import NIOQUICHelpers

/// Dialing the members that sort after this one.
///
/// Both ends could dial each other, and would then hold two connections. A
/// member dials only members whose id sorts after its own, and accepts the rest.
@available(anyAppleOS 27, *)
extension ReplicationHost {

  /// Keeps a connection open to every member this one dials, as the member list
  /// changes, until the calling task is cancelled.
  nonisolated func dialMembers() async {
    await withDiscardingTaskGroup { group in
      var dialing: Set<String> = []
      while !Task.isCancelled {
        for member in await coordinator.members() where member.id > coordinator.me && dialing.insert(member.id).inserted {
          group.addTask { await self.keepDialing(member.id) }
        }
        try? await Task.sleep(for: .seconds(1))
      }
    }
  }

  /// Dials `id` again each time its connection ends, until it leaves the group.
  private nonisolated func keepDialing(_ id: String) async {
    while !Task.isCancelled {
      guard let member = await coordinator.members().first(where: { $0.id == id }) else { return }
      await dial(member)
      try? await Task.sleep(for: Self.redialInterval)
    }
  }

  /// Runs one connection to `member`, from dial to close.
  private nonisolated func dial(_ member: Member) async {
    guard let endpoint = ReplicationEndpoint(member.address) else {
      logger.warning("member \(member.id) has no usable address: \(member.address)")
      return
    }
    let datagram: any Channel
    let stream: SyncStreamChannel
    do {
      (datagram, stream) = try await open(endpoint)
    } catch {
      logger.debug("cannot reach member \(member.id): \(error)")
      return
    }
    let connection = stream.channel.parent
    await converse(on: stream, expecting: member.id)
    if let connection {
      await withTaskGroup(of: Void.self) { group in
        group.addTask { try? await connection.closeFuture.get() }
        group.addTask { try? await Task.sleep(for: .seconds(2)) }
        await group.next()
        group.cancelAll()
      }
    }
    try? await datagram.close()
  }

  /// Resolves the endpoint, completes the QUIC handshake and opens the
  /// connection's bidirectional stream.
  private nonisolated func open(_ endpoint: ReplicationEndpoint) async throws -> (any Channel, SyncStreamChannel) {
    let remote = try await Self.resolve(endpoint)
    let configuration = trust.configuration(applicationProtocol: ReplicationTLS.applicationProtocol)
    let logger = logger
    let (datagram, dialer) = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
      .bind(host: remote.protocol == .inet ? "0.0.0.0" : "::", port: 0) { channel in
        channel.eventLoop.makeCompletedFuture {
          let (handler, dialer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
            channel: channel,
            quicConfiguration: configuration,
            logger: logger,
            inboundStreamChannelInitializer: { channel -> EventLoopFuture<Never> in
              channel.eventLoop.makeFailedFuture(SyncTransportError.unexpectedStream)
            }
          )
          try channel.pipeline.syncOperations.addHandler(handler)
          return (channel, dialer)
        }
      }
    do {
      let connection = try await dialer.createNewConnection(
        serverName: endpoint.host,
        remoteAddress: remote,
        inboundStreamInitializer: { channel -> EventLoopFuture<Never> in
          channel.eventLoop.makeFailedFuture(SyncTransportError.unexpectedStream)
        }
      )
      let stream = try await connection.createBidirectionalStream { parameters in
        SyncStreamChannel.wrapping(parameters.channel)
      }
      return (datagram, stream)
    } catch {
      try? await datagram.close()
      throw SyncTransportError.connection(String(describing: error))
    }
  }

  /// Resolves with the system resolver, which blocks, so it runs on the global
  /// executor rather than the caller's.
  @concurrent
  private static func resolve(_ endpoint: ReplicationEndpoint) async throws -> SocketAddress {
    do { return try SocketAddress.makeAddressResolvingHost(endpoint.host, port: endpoint.port) } catch {
      throw SyncTransportError.connection("cannot resolve \(endpoint.host): \(error)")
    }
  }
}
