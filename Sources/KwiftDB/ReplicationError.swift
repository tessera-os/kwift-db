import Foundation

/// Why a replicated operation did not commit.
public enum ReplicationError: Error, Sendable, Equatable, LocalizedError {
  /// No leader is known, this member is not the leader it needed to be, or
  /// leadership changed before the entry committed (spec section 9.6). The
  /// caller may retry.
  case unavailable
  /// This member is a follower, and the operation runs on the leader only
  /// (spec section 9.6). `leader` is the member that is.
  case notLeader(leader: String)
  /// The leader did not answer or commit in time. The write may or may not
  /// have committed; sync clients resend and learn which.
  case timedOut
  /// The leader refused the forwarded request for this reason.
  case refused(String)

  /// The text a `forwarded` reply carries for `unavailable`.
  static let unavailableText = "Unavailable"

  public var errorDescription: String? {
    switch self {
    case .unavailable: "Unavailable"
    case .notLeader(let leader): "Not the leader; the leader is \(leader)"
    case .timedOut: "Replication timed out"
    case .refused(let reason): reason
    }
  }

  /// The error a `forwarded` reply's failure text stands for.
  init(forwardedFailure text: String) {
    self = text == Self.unavailableText ? .unavailable : .refused(text)
  }

  /// The failure text a leader puts in a `forwarded` reply for this error.
  var forwardedFailure: String {
    switch self {
    case .unavailable, .timedOut, .notLeader: Self.unavailableText
    case .refused(let reason): reason
    }
  }
}
