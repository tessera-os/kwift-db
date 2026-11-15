import Foundation

/// Foundation-backed random-access access to one KwiftDB file.
///
/// Files are kept open only while their owning `Store` is active. The write-ahead
/// log calls `synchronize()` before acknowledging a batch; immutable table
/// writers sync once after their final footer has been written.
internal final class StorageFile {
  private let url: URL
  private let handle: FileHandle
  private let writable: Bool
  private var closed = false

  private init(url: URL, handle: FileHandle, writable: Bool) {
    self.url = url
    self.handle = handle
    self.writable = writable
  }

  /// Creates a new empty file and opens it for updates.
  static func createNew(at url: URL) throws -> StorageFile {
    let fileManager = FileManager.default
    guard !fileManager.fileExists(atPath: url.path) else {
      throw KwiftDBError.io("file already exists: \(url.path)")
    }
    guard fileManager.createFile(atPath: url.path, contents: nil) else {
      throw KwiftDBError.io("could not create \(url.path)")
    }
    do {
      return StorageFile(url: url, handle: try FileHandle(forUpdating: url), writable: true)
    } catch {
      throw KwiftDBError.io("\(url.path): \(error)")
    }
  }

  /// Opens an existing file for appending and random-access writes.
  static func openForUpdating(at url: URL) throws -> StorageFile {
    do {
      return StorageFile(url: url, handle: try FileHandle(forUpdating: url), writable: true)
    } catch {
      throw KwiftDBError.io("\(url.path): \(error)")
    }
  }

  /// Opens an existing immutable file for random-access reads.
  static func openForReading(at url: URL) throws -> StorageFile {
    do {
      return StorageFile(url: url, handle: try FileHandle(forReadingFrom: url), writable: false)
    } catch {
      throw KwiftDBError.io("\(url.path): \(error)")
    }
  }

  /// Appends bytes at the current file end and answers the starting offset.
  func append(_ data: Data) throws -> UInt64 {
    guard writable, !closed else { throw KwiftDBError.io("file is not open for writing") }
    do {
      let offset = try handle.seekToEnd()
      try handle.write(contentsOf: data)
      return offset
    } catch {
      throw KwiftDBError.io("\(url.path): \(error)")
    }
  }

  /// Reads exactly `count` bytes beginning at `offset`.
  func read(at offset: UInt64, count: Int) throws -> Data {
    guard !closed else { throw KwiftDBError.io("file is closed") }
    guard count >= 0 else { throw KwiftDBError.io("negative read length") }
    do {
      try handle.seek(toOffset: offset)
      let data = try handle.read(upToCount: count) ?? Data()
      guard data.count == count else { throw KwiftDBError.corruptStorage("truncated file read") }
      return data
    } catch let error as KwiftDBError {
      throw error
    } catch {
      throw KwiftDBError.io("\(url.path): \(error)")
    }
  }

  /// Reads the entire file. Used for the small manifest and recovery log.
  func readWhole() throws -> Data {
    guard !closed else { throw KwiftDBError.io("file is closed") }
    do { return try Data(contentsOf: url) }
    catch { throw KwiftDBError.io("\(url.path): \(error)") }
  }

  /// The current physical size of this file.
  func size() throws -> UInt64 {
    do {
      let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
      guard let size = attributes[.size] as? NSNumber else {
        throw KwiftDBError.io("file size is unavailable for \(url.path)")
      }
      return size.uint64Value
    } catch let error as KwiftDBError {
      throw error
    } catch {
      throw KwiftDBError.io("\(url.path): \(error)")
    }
  }

  /// Flushes file contents to stable storage.
  func synchronize() throws {
    guard writable, !closed else { throw KwiftDBError.io("file is not open for synchronization") }
    do { try handle.synchronize() }
    catch { throw KwiftDBError.io("\(url.path): \(error)") }
  }

  /// Removes bytes after `offset`, used to discard a torn unacknowledged log tail.
  func truncate(to offset: UInt64) throws {
    guard writable, !closed else { throw KwiftDBError.io("file is not open for truncation") }
    do {
      try handle.truncate(atOffset: offset)
      try handle.synchronize()
    } catch {
      throw KwiftDBError.io("\(url.path): \(error)")
    }
  }

  /// Closes this file handle exactly once.
  func close() throws {
    guard !closed else { return }
    closed = true
    do { try handle.close() }
    catch { throw KwiftDBError.io("\(url.path): \(error)") }
  }

  deinit {
    if !closed { try? handle.close() }
  }
}
