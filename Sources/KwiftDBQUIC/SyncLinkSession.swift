import KwiftDB
import Logging
import NIOCore
import NIOPosix
import NIOQUIC
import NIOQUICHelpers

/// One dialed QUIC connection of a `SyncLink` and its session stream.
///
/// Each session binds its own UDP socket, so closing the session releases
/// everything it holds. Writes go through an ordered queue that a single task
/// drains into the stream, so frames leave in the order the link produced
/// them even though the link's actor may interleave its calls.
@available(anyAppleOS 27, *)
final class SyncLinkSession: Sendable {
  private typealias Multiplexer = QUICHandler<QUICStreamChannels>.ConnectionMultiplexer<Never>

  /// How long a closing session waits for CONNECTION_CLOSE to go out before
  /// it closes the socket anyway.
  private static let closeGrace: Duration = .seconds(2)

  private let datagram: any Channel
  private let stream: SyncStreamChannel
  private let outgoing: AsyncStream<ByteBuffer>
  private let outgoingSink: AsyncStream<ByteBuffer>.Continuation

  private init(datagram: any Channel, stream: SyncStreamChannel) {
    self.datagram = datagram
    self.stream = stream
    (outgoing, outgoingSink) = AsyncStream.makeStream(of: ByteBuffer.self, bufferingPolicy: .unbounded)
  }

  /// Resolves `host`, completes the QUIC handshake and opens the session's
  /// bidirectional stream.
  ///
  /// - Throws: `SyncTransportError.connection` when any step fails.
  static func open(host: String, port: Int, trust: SyncTrust, logger: Logger) async throws -> SyncLinkSession {
    let remote = try await resolve(host, port: port)
    let configuration = trust.configuration
    let local = remote.protocol == .inet ? "0.0.0.0" : "::"

    let bound: (any Channel, Multiplexer)
    do {
      bound = try await DatagramBootstrap(group: MultiThreadedEventLoopGroup.singleton)
        .bind(host: local, port: 0) { channel in
          channel.eventLoop.makeCompletedFuture {
            let (handler, multiplexer) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
              channel: channel,
              quicConfiguration: configuration,
              logger: logger,
              inboundStreamChannelInitializer: { channel -> EventLoopFuture<Never> in
                channel.eventLoop.makeFailedFuture(SyncTransportError.unexpectedStream)
              }
            )
            try channel.pipeline.syncOperations.addHandler(handler)
            return (channel, multiplexer)
          }
        }
    } catch {
      throw SyncTransportError.connection(String(describing: error))
    }

    let (datagram, multiplexer) = bound
    do {
      let connection = try await multiplexer.createNewConnection(
        serverName: host,
        remoteAddress: remote,
        inboundStreamInitializer: { channel -> EventLoopFuture<Never> in
          // Sync streams are opened by the client only (spec section 8.6).
          channel.eventLoop.makeFailedFuture(SyncTransportError.unexpectedStream)
        }
      )
      let stream = try await connection.createBidirectionalStream { parameters in
        SyncStreamChannel.wrapping(parameters.channel)
      }
      return SyncLinkSession(datagram: datagram, stream: stream)
    } catch {
      try? await datagram.close()
      throw SyncTransportError.connection(String(describing: error))
    }
  }

  /// Resolves `host` with the system resolver, which blocks, so it runs on
  /// the global executor rather than the caller's.
  @concurrent
  private static func resolve(_ host: String, port: Int) async throws -> SocketAddress {
    do { return try SocketAddress.makeAddressResolvingHost(host, port: port) } catch {
      throw SyncTransportError.connection("cannot resolve \(host): \(error)")
    }
  }

  /// Queues bytes for the stream, after everything queued before them.
  func write(_ bytes: ByteBuffer) {
    outgoingSink.yield(bytes)
  }

  /// Runs the stream until it ends, passing each chunk read to `receive`.
  ///
  /// `receive` returns false to stop reading. Returns how the stream ended:
  /// clean when the host finished it or closed with code 0, a failure
  /// otherwise.
  func read(_ receive: @Sendable (ByteBuffer) async -> Bool) async -> SyncClose {
    let outgoing = outgoing
    do {
      return try await stream.executeThenClose { inbound, outbound in
        try await withThrowingTaskGroup(of: Void.self, returning: SyncClose.self) { group in
          group.addTask {
            for await bytes in outgoing { try await outbound.write(bytes) }
          }
          defer { group.cancelAll() }
          for try await bytes in inbound {
            guard await receive(bytes) else { return .clean }
          }
          return .clean
        }
      }
    } catch {
      return SyncClose(ending: error)
    }
  }

  /// Closes the QUIC connection with `close`, then the socket.
  ///
  /// The socket closes once the connection has gone, or after a short grace
  /// period, so CONNECTION_CLOSE reaches the host rather than leaving it to
  /// time the connection out.
  func close(_ close: SyncClose) async {
    outgoingSink.finish()
    if let connection = stream.channel.parent {
      stream.channel.closeConnection(close)
      await withTaskGroup(of: Void.self) { group in
        group.addTask { try? await connection.closeFuture.get() }
        group.addTask { try? await Task.sleep(for: Self.closeGrace) }
        await group.next()
        group.cancelAll()
      }
    }
    try? await datagram.close()
  }
}
