import Foundation
import KwiftDB
import Logging
import NIOCore
import NIOPosix
import NIOQUIC

/// Something a link reports to the program that owns it.
public enum SyncLinkEvent: Sendable {
  /// The session stream is open and `hello` has been sent.
  case connected
  /// The replica reported an event for a server message.
  case replica(ReplicaEvent)
  /// The session ended. A reason is present when it ended with a failure.
  case disconnected(String?)
}

/// Connects a `Replica` to a sync host over QUIC (spec section 8.6, `linkSync`).
///
/// `run` dials the host, opens one bidirectional stream and connects the
/// replica: `hello`, then every subscription and every pending mutation.
/// Server messages go to the replica and the resulting `ReplicaEvent`s come
/// out of `events`. After every server message the link writes whatever the
/// replica queued. `run` returns when the session ends; cancelling its task
/// closes the connection with code 0. To reconnect, call `run` again.
///
/// The program calls `send()` after its own `subscribe`, `unsubscribe` or
/// `mutate` on the replica, online or not; see `send()`.
///
/// ```swift
/// let link = SyncLink(replica: replica)
/// await withDiscardingTaskGroup { group in
///   group.addTask { await link.run(host: "sync.example.com", port: 4433,
///                                  credentials: .text(token), trust: .systemRoots) }
///   for await event in link.events { ... }
/// }
/// ```
@available(anyAppleOS 27, *)
public actor SyncLink {
  /// The replica this link carries; the program reads and writes through it.
  public let replica: Replica

  /// Every event of every session this link runs, in order.
  public nonisolated let events: AsyncStream<SyncLinkEvent>

  private let eventSink: AsyncStream<SyncLinkEvent>.Continuation
  private let logger: Logger
  private var session: SyncLinkSession?
  private var running = false
  private var reader = SyncFrameReader()
  /// Set when the link itself decides to end the session while reading.
  private var ending: SyncClose?

  /// Creates a link for `replica`. Nothing is dialed until `run`.
  public init(replica: Replica, logger: Logger = Logger(label: "kwiftdb.sync.link")) {
    self.replica = replica
    self.logger = logger
    (events, eventSink) = AsyncStream.makeStream(of: SyncLinkEvent.self, bufferingPolicy: .unbounded)
  }

  /// Whether a session is open.
  public var connected: Bool { session != nil }

  /// Runs one session: dials the host, sends what a new session needs and
  /// reads until the session ends or the calling task is cancelled.
  ///
  /// `events` reports `.connected` once `hello` is sent and `.disconnected`
  /// when the session ends. A link runs one session at a time.
  ///
  /// - Parameters:
  ///   - host: The host's name or address. A name is also the TLS server name.
  ///   - port: The host's UDP port.
  ///   - credentials: The value `hello` carries to the server's `authenticate`.
  ///   - trust: Which certificates the host may present.
  public func run(host: String, port: Int, credentials: Value, trust: SyncTrust) async {
    precondition(!running, "a SyncLink runs one session at a time")
    running = true
    defer { running = false }
    reader = SyncFrameReader()
    ending = nil

    let opened: SyncLinkSession
    do {
      opened = try await SyncLinkSession.open(host: host, port: port, trust: trust, logger: logger)
    } catch {
      eventSink.yield(.disconnected(String(describing: error)))
      return
    }
    guard !Task.isCancelled else {
      await opened.close(.clean)
      eventSink.yield(.disconnected(nil))
      return
    }
    do {
      // The session is registered only once hello is written, so a `send()`
      // while `connect` runs cannot put another frame ahead of hello.
      opened.write(try SyncFrameCodec.buffer(try await replica.connect(credentials: credentials)))
      session = opened
      eventSink.yield(.connected)
      await send()
    } catch {
      ending = .failure(String(describing: error))
    }

    var close = ending ?? .clean
    if ending == nil {
      let read = await withTaskGroup(of: SyncClose?.self) { group in
        group.addTask { await opened.read { bytes in await self.receive(bytes) } }
        group.addTask { await Self.beat(on: opened); return nil }
        // The heartbeat runs until cancelled, so the reader ends the session.
        var read = SyncClose.clean
        for await result in group {
          guard let result else { continue }
          read = result
          group.cancelAll()
        }
        return read
      }
      close = ending ?? (Task.isCancelled ? .clean : read)
    }
    session = nil
    await opened.close(close)
    try? await replica.disconnect()
    eventSink.yield(.disconnected(close.reason))
  }

  /// Writes a heartbeat every ``SyncTLS/keepAliveInterval`` until cancelled.
  ///
  /// The transport's own keepalive does not survive traffic arriving between
  /// its pings, so a quiet session would end at the idle timeout. The
  /// server's acknowledgement of each heartbeat keeps both ends' idle timers
  /// fresh.
  private static func beat(on session: SyncLinkSession) async {
    guard let frame = try? SyncFrameCodec.buffer([.heartbeat]) else { return }
    while true {
      do { try await Task.sleep(for: SyncTLS.keepAliveInterval) } catch { return }
      session.write(frame)
    }
  }

  /// Reports the replica's local events, then writes every message it
  /// queued.
  ///
  /// Call it after `subscribe`, `unsubscribe` or `mutate` on the replica, also
  /// while offline: the local events (a subscription's first value, a
  /// mutation's speculative effect) come out of `events` at once. Messages wait
  /// in the replica until a session is open; a replica that is not connected
  /// queues none, since its next `connect` sends everything a session needs.
  public func send() async {
    for event in await replica.takeEvents() { eventSink.yield(.replica(event)) }
    guard let session else { return }
    let messages = await replica.takeOutgoing()
    guard !messages.isEmpty else { return }
    do { session.write(try SyncFrameCodec.buffer(messages)) } catch {
      ending = .failure(String(describing: error))
    }
  }

  /// Hands a chunk's messages to the replica. Returns false once the session
  /// should stop reading.
  private func receive(_ bytes: ByteBuffer) async -> Bool {
    do {
      for message in try reader.append(Array(bytes.readableBytesView)) {
        for event in try await replica.receive(message) { eventSink.yield(.replica(event)) }
        if case .refused = message {
          // The server ended the session; closing promptly frees it.
          ending = .clean
          return false
        }
        await send()
        if ending != nil { return false }
      }
      return true
    } catch {
      ending = .failure(String(describing: error))
      return false
    }
  }
}
