import Foundation

/// The database directories this process has open through ``Replica/open(directory:functions:retention:)``.
///
/// A directory's store numbers its write batches from its own manifest, so
/// two stores open on one directory both write batches under the same
/// sequence numbers and corrupt the sorted tables they flush. A claim keeps
/// one opener per directory: a second opener waits until the first closes.
actor DirectoryClaims {
  static let shared = DirectoryClaims()

  private var claimed: Set<String> = []
  private var waiters: [String: [CheckedContinuation<Void, Never>]] = [:]

  /// Waits until no one holds `directory`, then holds it.
  func claim(_ directory: URL) async {
    let path = Self.path(directory)
    if claimed.insert(path).inserted { return }
    await withCheckedContinuation { waiters[path, default: []].append($0) }
  }

  /// Hands `directory` to the longest waiter, or frees it.
  func release(_ directory: URL) {
    let path = Self.path(directory)
    if var queue = waiters[path], !queue.isEmpty {
      let next = queue.removeFirst()
      waiters[path] = queue.isEmpty ? nil : queue
      next.resume()
    } else {
      claimed.remove(path)
    }
  }

  private static func path(_ directory: URL) -> String {
    directory.standardizedFileURL.resolvingSymlinksInPath().path
  }
}
