import NIOQUIC

/// Constants and TLS settings shared by `SyncHost` and `SyncLink`.
///
/// Sync runs over QUIC with TLS 1.3. Every connection negotiates the ALPN
/// protocol `kiradb` (spec section 8.6); a peer that offers anything else is
/// refused during the handshake.
public enum SyncTLS {
  /// The ALPN protocol identifier of a sync connection.
  public static let applicationProtocol = "kiradb"

  /// How long a client waits on a silent server before dropping the session.
  public static let idleTimeout: Duration = .seconds(10)

  /// How often a client pings the server to keep the session alive.
  public static let keepAliveInterval: Duration = .seconds(4)
}

/// The certificate a `SyncHost` presents to links.
///
/// SwiftNIO QUIC reads both files when the host starts. Keys are P-256.
public struct SyncHostCertificate: Sendable {
  /// The name the certificate is issued for, such as `sync.example.com`.
  public var serverName: String
  /// A PEM file holding the leaf certificate first, then any intermediates.
  public var chainPath: String
  /// A PEM file holding the leaf's private key.
  public var privateKeyPath: String

  /// Describes a certificate stored as PEM files.
  public init(serverName: String, chainPath: String, privateKeyPath: String) {
    self.serverName = serverName
    self.chainPath = chainPath
    self.privateKeyPath = privateKeyPath
  }
}

/// Which certificates a `SyncLink` accepts from its host.
public enum SyncTrust: Sendable {
  /// The platform's trusted roots, for hosts with a publicly issued certificate.
  case systemRoots
  /// Only chains ending at a root in this PEM file, for private deployments
  /// and tests.
  case roots(path: String)
}

/// A failure of the QUIC transport itself, as opposed to the sync protocol.
public enum SyncTransportError: Error, Sendable, CustomStringConvertible {
  /// The host could not bind its UDP socket or load its certificate.
  case listener(String)
  /// The link could not resolve, dial or open its session stream.
  case connection(String)
  /// The host opened a stream to the link; sync never does.
  case unexpectedStream

  public var description: String {
    switch self {
    case .listener(let reason): "sync host failed: \(reason)"
    case .connection(let reason): "sync connection failed: \(reason)"
    case .unexpectedStream: "the sync host opened a stream"
    }
  }
}

@available(anyAppleOS 27, *)
extension SyncHostCertificate {
  /// The SwiftNIO QUIC server configuration for this certificate.
  var configuration: QUICConfiguration { configuration(applicationProtocol: SyncTLS.applicationProtocol) }

  /// The server configuration negotiating `applicationProtocol`.
  func configuration(applicationProtocol: String) -> QUICConfiguration {
    var configuration = QUICConfiguration.forServer(
      serverName: serverName,
      authenticationConfiguration: .x509Certificates(
        certificateChainFilePath: chainPath,
        privateKeyFilePath: privateKeyPath
      ),
      applicationProtocols: [applicationProtocol]
    )
    // The host pings too: a quiet connection stays open even when the client's
    // keepalive does not run, and a vanished client is dropped after the
    // idle timeout instead of holding its subscriptions.
    configuration.maxIdleTimeout = SyncTLS.idleTimeout
    configuration.keepAliveInterval = SyncTLS.keepAliveInterval
    return configuration
  }
}

@available(anyAppleOS 27, *)
extension SyncTrust {
  /// The SwiftNIO QUIC client configuration for this trust.
  var configuration: QUICConfiguration { configuration(applicationProtocol: SyncTLS.applicationProtocol) }

  /// The client configuration negotiating `applicationProtocol`.
  ///
  /// The client pings every ``SyncTLS/keepAliveInterval`` and gives up on a
  /// silent server after ``SyncTLS/idleTimeout``, so a restarted server is
  /// noticed in seconds rather than after the 30-second default.
  func configuration(applicationProtocol: String) -> QUICConfiguration {
    let roots: String? = switch self {
    case .systemRoots: nil
    case .roots(let path): path
    }
    var configuration = QUICConfiguration.forClient(
      verificationConfiguration: .x509Certificates(trustRootsFilePath: roots),
      applicationProtocols: [applicationProtocol]
    )
    configuration.maxIdleTimeout = SyncTLS.idleTimeout
    configuration.keepAliveInterval = SyncTLS.keepAliveInterval
    return configuration
  }
}
