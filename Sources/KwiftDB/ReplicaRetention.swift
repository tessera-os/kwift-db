/// What a replica does with rows the server stops holding for it.
///
/// The server sends `release` when no subscription reads a range any more
/// (KiraDB spec section 8.4), and when a new session's subscriptions are
/// ready, the replica's ranges from earlier sessions that nothing holds are
/// stale (section 8.5).
public enum ReplicaRetention: Sendable, Equatable {
  /// Removes released and stale rows, so the replica holds exactly what its
  /// subscriptions read. The spec's default.
  case exact
  /// Keeps released and stale rows as a cache that queries still read
  /// offline and before they are ready. A range held again is replaced by the
  /// server's pairs (`hold`), so cached rows are only ever seen by a query
  /// that is not ready, which `Replica.isReady` and `ReplicaEvent.ready`
  /// report. The cache is never trimmed.
  case keepReleased
}
