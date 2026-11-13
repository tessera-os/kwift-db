import Foundation
import KwiftDB

/// The first frame of a replication connection: who is speaking and the shared
/// secret that proves it belongs to the group.
///
/// The dialing member sends its `hello` first; the member it dials checks it and
/// answers with its own. Frames are varint-prefixed value documents, like every
/// other message of the connection.
struct ReplicationHello: Sendable, Equatable {
  /// The sender's member id.
  var member: String
  /// The group's shared secret.
  var secret: [UInt8]

  init(member: String, secret: ReplicationSecret) {
    self.member = member
    self.secret = secret.bytes
  }

  /// Decodes a frame's payload.
  init(payload: Data) throws {
    guard case .document(let document) = try ValueCodec.decode(payload), case .text("hello")? = document["type"],
      case .text(let member)? = document["member"], case .bytes(let secret)? = document["secret"]
    else { throw KwiftDBError.invalidEncoding("the first replication frame is not a hello") }
    self.member = member
    self.secret = secret
  }

  /// The frame to write.
  func frame() throws -> Data {
    let document = try Document([
      DocumentField("type", .text("hello")), DocumentField("member", .text(member)), DocumentField("secret", .bytes(secret)),
    ])
    return FrameSplitter.frame(try ValueCodec.encode(.document(document)))
  }
}

/// A member's address, `host:port` or `[ipv6]:port`.
struct ReplicationEndpoint: Sendable, Equatable {
  var host: String
  var port: Int

  init?(_ address: String) {
    guard let colon = address.lastIndex(of: ":"), let port = Int(address[address.index(after: colon)...]) else { return nil }
    var host = String(address[..<colon])
    if host.hasPrefix("["), host.hasSuffix("]") { host = String(host.dropFirst().dropLast()) }
    guard !host.isEmpty else { return nil }
    self.host = host
    self.port = port
  }
}
