import Foundation
import KwiftDB
import NIOCore

/// The conversation of one replication connection, from handshake to close.
///
/// A connection is one bidirectional stream carrying frames both ways: first
/// the two `hello`s, then `ReplicationMessage`s. Incoming messages go to the
/// coordinator; messages the coordinator wants sent to this peer arrive through
/// the host's link registry and are written in order.
@available(anyAppleOS 27, *)
struct ReplicationPeerSession {
  let coordinator: ReplicationCoordinator
  let host: ReplicationHost
  let secret: ReplicationSecret
  /// The member the dialing end expects to reach, or nil for the end that was dialed.
  let expecting: String?

  /// How long a connection has to complete its handshake.
  static let helloTimeout: Duration = .seconds(10)

  /// Runs the connection until it ends and returns how to close it.
  ///
  /// A connection whose handshake is not complete after `helloTimeout` is closed
  /// with code 1.
  func run(inbound: NIOAsyncChannelInboundStream<ByteBuffer>, outbound: NIOAsyncChannelOutboundWriter<ByteBuffer>) async -> SyncClose {
    let handshake = HandshakeSignal()
    return await withTaskGroup(of: SyncClose?.self) { group in
      group.addTask { await converse(inbound: inbound, outbound: outbound, handshake: handshake) }
      group.addTask {
        try? await Task.sleep(for: Self.helloTimeout)
        guard !Task.isCancelled, !(await handshake.completed) else { return nil }
        return .failure("no hello within \(Self.helloTimeout)")
      }
      var close = SyncClose.clean
      for await result in group {
        guard let result else { continue }
        close = result
        break
      }
      group.cancelAll()
      return close
    }
  }

  private func converse(
    inbound: NIOAsyncChannelInboundStream<ByteBuffer>, outbound: NIOAsyncChannelOutboundWriter<ByteBuffer>, handshake: HandshakeSignal
  ) async -> SyncClose {
    var iterator = inbound.makeAsyncIterator()
    var splitter = FrameSplitter()
    let peer: String
    var early: [Data]
    do {
      if expecting != nil { try await outbound.write(ByteBuffer(bytes: try ReplicationHello(member: coordinator.me, secret: secret).frame())) }
      early = []
      while early.isEmpty {
        guard let bytes = try await iterator.next() else { return .failure("the connection ended before its hello") }
        early = try splitter.append(Array(bytes.readableBytesView))
      }
      let hello = try ReplicationHello(payload: early.removeFirst())
      guard secret.matches(hello.secret) else { return .failure("replication secret mismatch") }
      let members = await coordinator.members()
      guard hello.member != coordinator.me, members.contains(where: { $0.id == hello.member }), expecting.map({ $0 == hello.member }) ?? true else {
        return .failure("\(hello.member) is not the member expected")
      }
      peer = hello.member
      if expecting == nil { try await outbound.write(ByteBuffer(bytes: try ReplicationHello(member: coordinator.me, secret: secret).frame())) }
    } catch {
      return SyncClose(ending: error)
    }

    await handshake.complete()
    let (queue, sink) = AsyncStream.makeStream(of: ReplicationMessage.self, bufferingPolicy: .unbounded)
    let link = await host.register(peer, sink)
    let close = await withTaskGroup(of: Void.self, returning: SyncClose.self) { group in
      group.addTask {
        for await message in queue {
          do { try await outbound.write(ByteBuffer(bytes: try ReplicationFrameCodec.encode(message))) } catch { break }
        }
        // Replaced by a newer link to this peer: finish our side so the peer
        // ends this connection too.
        outbound.finish()
      }
      defer { group.cancelAll() }
      do {
        for frame in early { try await coordinator.receive(from: peer, message: try ReplicationMessage.decodeValue(frame)) }
        while let bytes = try await iterator.next() {
          for frame in try splitter.append(Array(bytes.readableBytesView)) {
            try await coordinator.receive(from: peer, message: try ReplicationMessage.decodeValue(frame))
          }
        }
        return .clean
      } catch {
        return Task.isCancelled ? .clean : SyncClose(ending: error)
      }
    }
    await host.unregister(peer, link: link)
    return close
  }
}

/// Whether a connection's handshake has finished, for its timeout to read.
private actor HandshakeSignal {
  private(set) var completed = false

  func complete() { completed = true }
}
