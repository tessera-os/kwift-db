import KwiftDB
import NIOCore
import NIOQUICHelpers

/// The stream that carries one sync session: raw bytes both ways.
///
/// A sync session is exactly one bidirectional QUIC stream (spec section 8.6).
/// Its bytes are varint-framed `SyncMessage` values (`SyncFrameCodec`).
typealias SyncStreamChannel = NIOAsyncChannel<ByteBuffer, ByteBuffer>

/// How a session's QUIC connection is closed.
///
/// Code 0 is a clean end. Code 1 reports a failure, and `reason` carries its
/// text to the peer in the CONNECTION_CLOSE frame.
struct SyncClose: Sendable, Equatable {
  var code: UInt64
  var reason: String?

  static let clean = SyncClose(code: 0, reason: nil)

  static func failure(_ reason: String) -> SyncClose { SyncClose(code: 1, reason: reason) }

  /// The close a peer reported, read from the error that ended a read.
  ///
  /// An application close with code 0 is clean. Anything else is a failure
  /// described by the peer's reason or by the error itself.
  init(ending error: any Error) {
    if let closed = error as? QUICConnectionError {
      if closed.isApplication && closed.code == 0 {
        self = .clean
      } else {
        self = .failure(closed.reason.isEmpty ? "connection closed with code \(closed.code)" : closed.reason)
      }
    } else {
      self = .failure(String(describing: error))
    }
  }

  init(code: UInt64, reason: String?) {
    self.code = code
    self.reason = reason
  }
}

extension SyncStreamChannel {
  /// Wraps a new QUIC stream channel for async reads and writes.
  ///
  /// Must run on the channel's event loop, inside a stream initializer.
  /// Outbound half-closure lets a side finish writing and keep reading.
  static func wrapping(_ channel: any Channel) -> EventLoopFuture<SyncStreamChannel> {
    channel.eventLoop.makeCompletedFuture {
      try SyncStreamChannel(
        wrappingChannelSynchronously: channel,
        configuration: .init(
          isOutboundHalfClosureEnabled: true,
          inboundType: ByteBuffer.self,
          outboundType: ByteBuffer.self
        )
      )
    }
  }
}

extension Channel {
  /// Closes the QUIC connection this stream channel belongs to.
  ///
  /// Sends CONNECTION_CLOSE with the application code and reason. Every
  /// stream of the connection ends, on both sides.
  func closeConnection(_ close: SyncClose) {
    guard let connection = parent, let code = QUICApplicationErrorCode(close.code) else { return }
    connection.triggerUserOutboundEvent(
      QUICCloseConnectionEvent(code: code, reasonPhrase: close.reason),
      promise: nil
    )
  }

  /// Stops a stream the session does not use, with code 0 (spec section 8.6).
  func stopUnusedStream() {
    if let code = QUICApplicationErrorCode(0) {
      triggerUserOutboundEvent(QUICStopSendingEvent(code: code), promise: nil)
    }
    close(promise: nil)
  }
}

extension SyncFrameCodec {
  /// Encodes messages as consecutive frames in one buffer.
  static func buffer(_ messages: [SyncMessage]) throws -> ByteBuffer {
    var buffer = ByteBuffer()
    for message in messages { buffer.writeBytes(try encode(message)) }
    return buffer
  }
}
