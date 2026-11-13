/// What one refresh of a replica's local view produced: each subscribed
/// query's value, the storage ranges it read, and the keys pending mutations
/// wrote.
struct RefreshedView: Sendable {
  var values: [Number: Value]
  var ranges: [Number: [KeyRange]]
  var pendingWrites: [KeyRange]
  /// The subscribed queries that ran again rather than keeping their result.
  var reran: [String]
}
