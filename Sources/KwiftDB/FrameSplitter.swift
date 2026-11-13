import Foundation

/// Cuts a byte stream into frames: a base-128 variable-length byte count
/// followed by that many payload bytes (spec section 8.3).
///
/// Sync and replication transports both frame their messages this way. The
/// splitter holds the partial frame at the end of what it has been given, so
/// a transport may pass bytes in whatever chunks the network delivers.
public struct FrameSplitter: Sendable {
  private var pending: [UInt8] = []

  /// Creates a splitter with nothing buffered.
  public init() {}

  /// Wraps a payload in its length prefix.
  public static func frame(_ payload: Data) -> Data {
    var frame = Data(DatabaseUInt(UInt64(payload.count)).varintBytes())
    frame.append(payload)
    return frame
  }

  /// Adds network bytes and returns the payload of every frame now complete.
  ///
  /// - Throws: `KwiftDBError.invalidEncoding` when a length prefix is not a
  ///   valid count.
  public mutating func append(_ bytes: [UInt8]) throws -> [Data] {
    pending.append(contentsOf: bytes)
    var payloads: [Data] = []
    while let payload = try takePayload() { payloads.append(Data(payload)) }
    return payloads
  }

  private mutating func takePayload() throws -> [UInt8]? {
    guard !pending.isEmpty else { return nil }
    var offset = 0
    let length: DatabaseUInt
    do { length = try DatabaseUInt.readVarint(from: pending, offset: &offset) }
    catch KwiftDBError.invalidEncoding("truncated variable-length integer") { return nil }
    guard let count = length.intValue, count >= 0 else {
      throw KwiftDBError.invalidEncoding("frame is too large")
    }
    guard pending.count - offset >= count else { return nil }
    let payload = Array(pending[offset..<(offset + count)])
    pending.removeFirst(offset + count)
    return payload
  }
}
