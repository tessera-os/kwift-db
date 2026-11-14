import Foundation

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

/// Holds an exclusive operating-system lock for one database directory.
/// The lock file is intentionally left in place after closing: removing it
/// could let another opener lock a new file while a waiter still holds the old one.
internal final class StoreDirectoryLock {
  private var descriptor: Int32?

  init(directory: URL) throws {
    #if canImport(Darwin)
      let lockURL = directory.appendingPathComponent(".kwiftdb.lock", isDirectory: false)
      let descriptor = lockURL.path.withCString {
        Self.open($0, O_CREAT | O_RDWR | O_CLOEXEC | O_EXLOCK | O_NONBLOCK, mode_t(0o600))
      }
      guard descriptor >= 0 else {
        let code = errno
        if code == EWOULDBLOCK || code == EAGAIN {
          throw KwiftDBError.io("database directory is already open: \(directory.path)")
        }
        throw Self.failure("open", path: lockURL.path, code: code)
      }
      self.descriptor = descriptor
    #elseif canImport(Glibc)
      let lockURL = directory.appendingPathComponent(".kwiftdb.lock", isDirectory: false)
      let descriptor = lockURL.path.withCString {
        Self.open($0, O_CREAT | O_RDWR | O_CLOEXEC, mode_t(0o600))
      }
      guard descriptor >= 0 else {
        throw Self.failure("open", path: lockURL.path, code: errno)
      }
      guard Self.lock(descriptor, operation: LOCK_EX | LOCK_NB) == 0 else {
        let code = errno
        _ = Self.close(descriptor)
        if code == EWOULDBLOCK || code == EAGAIN {
          throw KwiftDBError.io("database directory is already open: \(directory.path)")
        }
        throw Self.failure("lock", path: lockURL.path, code: code)
      }
      self.descriptor = descriptor
    #else
      // Targets without POSIX file locks in this package's support matrix have
      // no separate processes that can share this local database directory.
      self.descriptor = nil
    #endif
  }

  func close() throws {
    guard let descriptor else { return }
    self.descriptor = nil
    #if canImport(Darwin)
      let closeResult = Self.close(descriptor)
      if closeResult != 0 { throw Self.failure("close", path: "database directory lock", code: errno) }
    #elseif canImport(Glibc)
      let unlockResult = Self.lock(descriptor, operation: LOCK_UN)
      let unlockError = unlockResult == 0 ? nil : errno
      let closeResult = Self.close(descriptor)
      let closeError = closeResult == 0 ? nil : errno
      if let code = unlockError ?? closeError {
        throw Self.failure("close", path: "database directory lock", code: code)
      }
    #endif
  }

  deinit { try? close() }

  #if canImport(Darwin)
    private static func open(_ path: UnsafePointer<CChar>, _ flags: Int32, _ mode: mode_t) -> Int32 {
      Darwin.open(path, flags, mode)
    }

    private static func close(_ descriptor: Int32) -> Int32 { Darwin.close(descriptor) }

    private static func failure(_ operation: String, path: String, code: Int32) -> KwiftDBError {
      KwiftDBError.io("could not \(operation) \(path): \(String(cString: Darwin.strerror(code)))")
    }
  #elseif canImport(Glibc)
    private static func open(_ path: UnsafePointer<CChar>, _ flags: Int32, _ mode: mode_t) -> Int32 {
      Glibc.open(path, flags, mode)
    }

    private static func lock(_ descriptor: Int32, operation: Int32) -> Int32 {
      Glibc.flock(descriptor, operation)
    }

    private static func close(_ descriptor: Int32) -> Int32 { Glibc.close(descriptor) }

    private static func failure(_ operation: String, path: String, code: Int32) -> KwiftDBError {
      KwiftDBError.io("could not \(operation) \(path): \(String(cString: Glibc.strerror(code)))")
    }
  #endif
}
