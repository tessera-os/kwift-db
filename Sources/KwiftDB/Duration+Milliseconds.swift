extension Duration {
  /// The duration in whole milliseconds, for logs.
  var milliseconds: Int64 {
    let (seconds, attoseconds) = components
    return seconds * 1_000 + attoseconds / 1_000_000_000_000_000
  }
}
