/// Constants shared by the replication transport's two ends.
///
/// Members talk over QUIC with TLS 1.3. Every connection negotiates the ALPN
/// protocol `kiradb-replication` (spec section 9.9); a peer that offers
/// anything else is refused during the handshake.
public enum ReplicationTLS {
  /// The ALPN protocol identifier of a replication connection.
  public static let applicationProtocol = "kiradb-replication"
}

/// The secret every member of a group shares (spec section 9.9).
///
/// A member proves it belongs to the group by sending the secret in the first
/// frame of a connection, and the member it dials proves it back. TLS carries
/// the frame, and each end verifies the other's certificate, so the secret is
/// never in the clear and is never sent to a peer whose certificate is not
/// trusted.
public struct ReplicationSecret: Sendable {
  let bytes: [UInt8]

  /// Creates a secret from its text.
  public init(_ text: String) { bytes = Array(text.utf8) }

  /// Whether `candidate` is this secret, compared in time that does not depend
  /// on where they differ.
  func matches(_ candidate: [UInt8]) -> Bool {
    var difference = UInt8(truncatingIfNeeded: bytes.count ^ candidate.count)
    for (index, byte) in bytes.enumerated() {
      difference |= byte ^ (index < candidate.count ? candidate[index] : 0)
    }
    return difference == 0
  }
}
