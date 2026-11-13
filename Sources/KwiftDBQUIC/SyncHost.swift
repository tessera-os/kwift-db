import Foundation
import KwiftDB
import Logging
import NIOCore
import NIOPosix
import NIOQUIC

/// Serves a `SyncServer` to replicas over QUIC (spec section 8.6, `hostSync`).
///
/// `bind` opens the UDP socket; `run` serves until its task is cancelled.
/// The host accepts QUIC connections that negotiate `kiradb`. On each
/// connection the first bidirectional stream the client opens carries that
/// client's sync session; any other stream is stopped with code 0. The host
/// feeds the session's frames to the engine and writes back every message the
/// engine queues, including transitions caused by other clients' mutations
/// and by server-owned writes (`SyncServer.outboxSignals`).
///
/// A host consumes its server's `outboxSignals`, so one server has at most
/// one host.
///
/// ```swift
/// let host = try await SyncHost.bind(server: server, port: 4433, certificate: certificate)
/// try await withThrowingDiscardingTaskGroup { group in
///   group.addTask { await host.run() }
///   // ...
/// }
/// ```
@available(anyAppleOS 27, *)
public actor SyncHost {
  private typealias Multiplexer = QUICHandler<QUICStreamChannels>.ConnectionMultiplexer<SyncStreamChannel>

  /// The UDP port the host is bound to.
  public nonisolated let port: Int

  private let server: SyncServer
  private let channel: any Channel
  private let multiplexer: Multiplexer
  private var sessions: [UUID: AsyncStream<SyncHostInput>.Continuation] = [:]
  /// Where the host reports refusals and sessions that end in failure.
  nonisolated let logger: Logger

  private init(server: SyncServer, channel: any Channel, multiplexer: Multiplexer, port: Int, logger: Logger) {
    self.server = server
    self.logger = logger
    self.channel = channel
    self.multiplexer = multiplexer
    self.port = port
  }

  /// Binds the host's UDP socket. Nothing is served until `run`.
  ///
  /// - Parameters:
  ///   - server: The engine holding the database of record.
  ///   - address: The local address to bind; `::` takes every IPv6 and IPv4
  ///     address on dual-stack systems.
  ///   - port: The UDP port; 0 lets the system choose (read it from `port`).
  ///   - certificate: The certificate presented to links.
  ///   - logger: Where SwiftNIO QUIC logs, and where the host reports
  ///     refusals and sessions that end in failure.
  /// - Throws: `SyncTransportError.listener` when the socket cannot be bound
  ///   or the certificate cannot be loaded.
  public static func bind(
    server: SyncServer,
    address: String = "::",
    port: Int,
    certificate: SyncHostCertificate,
    logger: Logger = Logger(label: "kwiftdb.sync.host")
  ) async throws -> SyncHost {
    let configuration = certificate.configuration
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
      return SyncHost(server: server, channel: channel, multiplexer: multiplexer, port: bound, logger: logger)
    } catch let error as SyncTransportError {
      throw error
    } catch {
      throw SyncTransportError.listener(String(describing: error))
    }
  }

  /// Serves connections until the calling task is cancelled.
  ///
  /// Cancellation ends every session, each closing its connection with
  /// code 0, and then closes the socket.
  public nonisolated func run() async {
    await withDiscardingTaskGroup { group in
      group.addTask { await self.relayOutboxSignals() }
      group.addTask { await self.accept() }
    }
    try? await channel.close()
  }

  func register(_ connection: UUID, _ inputs: AsyncStream<SyncHostInput>.Continuation) {
    sessions[connection] = inputs
  }

  func unregister(_ connection: UUID) {
    sessions.removeValue(forKey: connection)
  }

  /// Turns the engine's outbox signals into `drain` inputs for their sessions.
  private nonisolated func relayOutboxSignals() async {
    for await connection in server.outboxSignals {
      await signal(connection)
    }
  }

  private func signal(_ connection: UUID) {
    sessions[connection]?.yield(.drain)
  }

  private nonisolated func accept() async {
    await withDiscardingTaskGroup { group in
      for await connection in multiplexer.inboundConnections {
        group.addTask { await self.serve(connection) }
      }
    }
  }

  /// Runs a connection's session on its first stream and stops the others.
  private nonisolated func serve(_ connection: QUICConnection<SyncStreamChannel>) async {
    await withDiscardingTaskGroup { group in
      var adopted = false
      for await stream in connection.inboundStreams {
        if adopted {
          stream.channel.stopUnusedStream()
        } else {
          adopted = true
          group.addTask { await self.run(stream) }
        }
      }
    }
  }

  /// Reads the session stream into a `SyncHostSession` and closes the
  /// connection with the session's outcome.
  private nonisolated func run(_ stream: SyncStreamChannel) async {
    let (queue, inputs) = AsyncStream.makeStream(of: SyncHostInput.self, bufferingPolicy: .unbounded)
    let close: SyncClose
    do {
      close = try await stream.executeThenClose { inbound, outbound in
        await withTaskGroup(of: Void.self, returning: SyncClose.self) { group in
          group.addTask {
            do {
              for try await bytes in inbound { inputs.yield(.bytes(bytes)) }
              inputs.yield(.ended(.clean))
            } catch {
              inputs.yield(.ended(SyncClose(ending: error)))
            }
          }
          var session = SyncHostSession(server: server, host: self, outbound: outbound, inputs: inputs)
          let close = await session.run(queue)
          group.cancelAll()
          return close
        }
      }
    } catch {
      close = .failure(String(describing: error))
    }
    inputs.finish()
    if close != .clean {
      logger.warning("sync session failed", metadata: ["code": "\(close.code)", "reason": "\(close.reason ?? "")"])
    }
    stream.channel.closeConnection(close)
  }
}
