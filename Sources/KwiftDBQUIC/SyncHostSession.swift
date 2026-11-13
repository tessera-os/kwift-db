import Foundation
import KwiftDB
import Logging
import NIOCore

/// One input to a host session's serial loop.
///
/// The stream's bytes and the engine's outbox signals arrive from different
/// tasks. Funnelling them through one ordered queue means frames are decoded
/// in arrival order and replies are written in the order the engine queued
/// them.
enum SyncHostInput: Sendable {
  /// Bytes read from the session stream, in order.
  case bytes(ByteBuffer)
  /// The engine may have queued messages for this session.
  case drain
  /// The peer finished the stream, the read failed, or the host is stopping.
  case ended(SyncClose)
}

/// The sync session one client's first stream carries, run to its end.
///
/// The first message must be `hello`; the engine then accepts or refuses the
/// client. After a refusal the session finishes its side of the stream and
/// waits for the client to close the connection, so the `refused` frame is
/// delivered rather than cut off by CONNECTION_CLOSE.
@available(anyAppleOS 27, *)
struct SyncHostSession {
  /// How long a refused client has to close before the host closes for it.
  static let refusalGrace: Duration = .seconds(5)

  let server: SyncServer
  let host: SyncHost
  let outbound: NIOAsyncChannelOutboundWriter<ByteBuffer>
  let inputs: AsyncStream<SyncHostInput>.Continuation

  private var reader = SyncFrameReader()
  private var connection: UUID?
  private var refused = false

  init(
    server: SyncServer,
    host: SyncHost,
    outbound: NIOAsyncChannelOutboundWriter<ByteBuffer>,
    inputs: AsyncStream<SyncHostInput>.Continuation
  ) {
    self.server = server
    self.host = host
    self.outbound = outbound
    self.inputs = inputs
  }

  /// Handles inputs until the session ends, then returns how to close.
  ///
  /// Cancellation ends the session cleanly.
  mutating func run(_ queue: AsyncStream<SyncHostInput>) async -> SyncClose {
    var close = SyncClose.clean
    loop: for await input in queue {
      switch input {
      case .bytes(let bytes):
        if let ending = await receive(bytes) { close = ending; break loop }
      case .drain:
        if let ending = await drain() { close = ending; break loop }
      case .ended(let ending):
        close = ending
        break loop
      }
      if refused { break loop }
    }
    if refused {
      await Self.awaitEnd(of: queue, within: Self.refusalGrace)
      close = .clean
    }
    if let connection {
      await server.disconnect(connection)
      await host.unregister(connection)
    }
    return close
  }

  /// Decodes bytes and hands each message to the engine.
  private mutating func receive(_ bytes: ByteBuffer) async -> SyncClose? {
    let messages: [SyncMessage]
    do { messages = try reader.append(Array(bytes.readableBytesView)) } catch {
      return .failure("malformed frames: \(error)")
    }
    for message in messages {
      // After a refusal the client has nothing left to say.
      guard !refused else { return nil }
      guard let connection else {
        if let ending = await open(with: message) { return ending }
        continue
      }
      do {
        try await deliver(try await server.receive(message, from: connection))
      } catch {
        return .failure(String(describing: error))
      }
    }
    return nil
  }

  /// Handles the first message, which must be `hello`.
  private mutating func open(with message: SyncMessage) async -> SyncClose? {
    guard case .hello(let client, let credentials) = message else {
      return await refuse([.refused(reason: "malformed message")])
    }
    switch await server.connect(client: client, credentials: credentials) {
    case .accepted(let id, let welcome):
      connection = id
      await host.register(id, inputs)
      do { try await write([welcome] + (await server.drain(id))) } catch {
        return .failure(String(describing: error))
      }
      return nil
    case .refused(let refusal):
      return await refuse([refusal])
    }
  }

  /// Sends the refusal and finishes the stream; `run` then gives the client
  /// time to close.
  private mutating func refuse(_ messages: [SyncMessage]) async -> SyncClose? {
    do { try await write(messages) } catch { return .failure(String(describing: error)) }
    outbound.finish()
    refused = true
    return nil
  }

  /// Waits until the stream ends or `grace` passes, whichever is first.
  private static func awaitEnd(of queue: AsyncStream<SyncHostInput>, within grace: Duration) async {
    await withTaskGroup(of: Void.self) { group in
      group.addTask {
        for await input in queue {
          if case .ended = input { return }
        }
      }
      group.addTask { try? await Task.sleep(for: grace) }
      await group.next()
      group.cancelAll()
    }
  }

  /// Writes whatever the engine queued for this session.
  private mutating func drain() async -> SyncClose? {
    guard let connection, !refused else { return nil }
    do { try await deliver(await server.drain(connection)) } catch {
      return .failure(String(describing: error))
    }
    return nil
  }

  /// Writes the engine's messages. When they end with a refusal, such as
  /// `mutation out of order`, the session finishes its side of the stream and
  /// `run` waits for the client to close, as for a refused `hello`: closing at
  /// once would cut the `refused` frame off, and the client, never told why,
  /// would only see its next message fail.
  private mutating func deliver(_ messages: [SyncMessage]) async throws {
    try await write(messages)
    guard messages.contains(where: { if case .refused = $0 { true } else { false } }) else { return }
    outbound.finish()
    refused = true
  }

  private func write(_ messages: [SyncMessage]) async throws {
    guard !messages.isEmpty else { return }
    for case .refused(let reason) in messages {
      host.logger.warning("sync session refused", metadata: ["reason": "\(reason)"])
    }
    try await outbound.write(try SyncFrameCodec.buffer(messages))
  }
}
