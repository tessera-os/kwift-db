/// A mutual-exclusion gate for async code: one holder at a time, the rest wait
/// in arrival order.
///
/// Actors interleave at every `await`, so an actor method that must not overlap
/// itself, such as one that drives the replication state machine through
/// several suspensions, holds a gate for its whole run.
actor AsyncGate {
  private var held = false
  private var waiters: [CheckedContinuation<Void, Never>] = []

  /// Waits until no other task holds the gate, then holds it.
  func acquire() async {
    if !held {
      held = true
      return
    }
    await withCheckedContinuation { waiters.append($0) }
  }

  /// Gives the gate to the longest waiter, or frees it when none waits.
  func release() {
    if waiters.isEmpty {
      held = false
    } else {
      waiters.removeFirst().resume()
    }
  }

  /// Runs `body` while holding the gate.
  func run<T: Sendable>(_ body: @Sendable () async throws -> T) async rethrows -> T {
    await acquire()
    defer { release() }
    return try await body()
  }
}
