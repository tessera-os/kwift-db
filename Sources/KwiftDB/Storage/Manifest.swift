import Foundation
#if os(Windows)
import WinSDK
#endif

/// The durable file inventory and last sequence number for one database.
///
/// The manifest is replaced as a complete text file, never edited in place.
/// `MANIFEST.next` is written and synchronized first, then renamed over
/// `MANIFEST`; the containing directory is synchronized after that rename.
internal struct Manifest: Sendable {
  /// The next file number to allocate. Allocated numbers are never reused.
  var nextFile: DatabaseUInt
  /// The last write sequence represented by this database.
  var lastSequence: DatabaseUInt
  /// The active write-ahead log file number.
  var log: DatabaseUInt
  /// Sorted tables currently included in the database.
  var tables: [TableRecord]

  /// Allocates the next file number and advances the manifest counter.
  mutating func takeFileNumber() -> DatabaseUInt {
    let number = nextFile
    nextFile = nextFile.incremented()
    return number
  }

  /// Serializes the exact one-fact-per-line manifest format.
  func text() -> String {
    var lines = [
      "next-file \(nextFile)",
      "last-sequence \(lastSequence)",
      "log \(log)",
    ]
    for table in tables {
      lines.append(
        "table \(table.level) \(table.number) \(Self.hex(table.smallest)) \(Self.hex(table.largest))"
      )
    }
    return lines.joined(separator: "\n") + "\n"
  }

  /// Parses the text manifest, rejecting unknown facts and malformed keys.
  static func parse(_ text: String) throws -> Manifest {
    var manifest = Manifest(
      nextFile: .zero,
      lastSequence: .zero,
      log: .zero,
      tables: []
    )
    for (lineIndex, line) in text.split(omittingEmptySubsequences: false, whereSeparator: { $0 == "\n" }).enumerated() {
      guard !line.isEmpty else { continue }
      let words = line.split(omittingEmptySubsequences: false, whereSeparator: { $0 == " " })
      if words.count == 2 {
        let value = try DatabaseUInt(String(words[1]))
        switch words[0] {
        case "next-file": manifest.nextFile = value
        case "last-sequence": manifest.lastSequence = value
        case "log": manifest.log = value
        default: throw KwiftDBError.corruptStorage("unknown manifest fact on line \(lineIndex + 1)")
        }
      } else if words.count == 5 && words[0] == "table" {
        guard let level = Int(words[1]), level >= 0 else {
          throw KwiftDBError.corruptStorage("invalid table level on line \(lineIndex + 1)")
        }
        let number = try DatabaseUInt(String(words[2]))
        let smallest = try Self.bytes(hex: String(words[3]))
        let largest = try Self.bytes(hex: String(words[4]))
        manifest.tables.append(
          TableRecord(level: level, number: number, smallest: smallest, largest: largest)
        )
      } else {
        throw KwiftDBError.corruptStorage("invalid manifest line \(lineIndex + 1)")
      }
    }
    guard !manifest.log.isZero else { throw KwiftDBError.corruptStorage("manifest names no log") }
    return manifest
  }

  private static func hex(_ bytes: [UInt8]) -> String {
    let digits = Array("0123456789abcdef".utf8)
    var result = String()
    result.reserveCapacity(bytes.count * 2)
    for byte in bytes {
      result.append(Character(UnicodeScalar(digits[Int(byte >> 4)])))
      result.append(Character(UnicodeScalar(digits[Int(byte & 0x0f)])))
    }
    return result
  }

  private static func bytes(hex: String) throws -> [UInt8] {
    let source = Array(hex.utf8)
    guard source.count % 2 == 0 else { throw KwiftDBError.corruptStorage("odd-length manifest key") }
    var result: [UInt8] = []
    result.reserveCapacity(source.count / 2)
    for index in stride(from: 0, to: source.count, by: 2) {
      guard let high = nibble(source[index]), let low = nibble(source[index + 1]) else {
        throw KwiftDBError.corruptStorage("manifest key is not hexadecimal")
      }
      result.append((high << 4) | low)
    }
    return result
  }

  private static func nibble(_ byte: UInt8) -> UInt8? {
    switch byte {
    case 48...57: byte - 48
    case 65...70: byte - 65 + 10
    case 97...102: byte - 97 + 10
    default: nil
    }
  }
}

/// File names and durable replacement operations for the manifest.
internal enum ManifestFile {
  static let name = "MANIFEST"
  static let next = "MANIFEST.next"

  /// The initial manifest: file 1 is the log and file 2 is next to allocate.
  static func fresh() -> Manifest {
    Manifest(nextFile: DatabaseUInt(2), lastSequence: .zero, log: .one, tables: [])
  }

  /// Reads and parses `MANIFEST` in a database directory.
  static func load(from directory: URL) throws -> Manifest {
    let url = directory.appendingPathComponent(name, isDirectory: false)
    do { return try Manifest.parse(String(contentsOf: url, encoding: .utf8)) }
    catch let error as KwiftDBError { throw error }
    catch { throw KwiftDBError.io("could not read \(url.path): \(error)") }
  }

  /// Writes and syncs a complete replacement manifest before installing it.
  static func store(_ manifest: Manifest, in directory: URL) throws {
    let current = directory.appendingPathComponent(name, isDirectory: false)
    let next = directory.appendingPathComponent(self.next, isDirectory: false)
    let manager = FileManager.default
    do {
      if manager.fileExists(atPath: next.path) { try manager.removeItem(at: next) }
      guard manager.createFile(atPath: next.path, contents: Data(manifest.text().utf8)) else {
        throw KwiftDBError.io("could not create \(next.path)")
      }
      let file = try FileHandle(forUpdating: next)
      try file.synchronize()
      try file.close()
      if manager.fileExists(atPath: current.path) {
        _ = try manager.replaceItemAt(current, withItemAt: next)
      } else {
        try manager.moveItem(at: next, to: current)
      }
      try synchronizeDirectory(directory)
    } catch let error as KwiftDBError {
      throw error
    } catch {
      throw KwiftDBError.io("could not store the manifest in \(directory.path): \(error)")
    }
  }
}

/// Flushes a directory's entries, so a rename into it survives a crash.
///
/// `FileHandle` cannot open a directory, so each platform uses its own call:
/// - POSIX systems (Apple, Linux, Android) open the directory and `fsync` it.
/// - Windows opens it with `FILE_FLAG_BACKUP_SEMANTICS`, the only way to get a
///   directory handle, and calls `FlushFileBuffers`. NTFS journals metadata,
///   so a failure to open the handle (for example on a share that refuses it)
///   is not treated as an error.
/// - WASI has no directory sync; the host runtime owns durability there.
func synchronizeDirectory(_ directory: URL) throws {
  #if os(Windows)
  let handle = directory.withUnsafeFileSystemRepresentation { path -> HANDLE? in
    guard let path else { return nil }
    return String(cString: path).withCString(encodedAs: UTF16.self) {
      CreateFileW($0, DWORD(GENERIC_READ), DWORD(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE),
                  nil, DWORD(OPEN_EXISTING), DWORD(FILE_FLAG_BACKUP_SEMANTICS), nil)
    }
  }
  guard let handle, handle != INVALID_HANDLE_VALUE else { return }
  defer { CloseHandle(handle) }
  _ = FlushFileBuffers(handle)
  #elseif os(WASI)
  _ = directory
  #else
  let descriptor = open(directory.path, O_RDONLY)
  guard descriptor >= 0 else {
    throw KwiftDBError.io("could not open directory \(directory.path): errno \(errno)")
  }
  defer { close(descriptor) }
  guard fsync(descriptor) == 0 else {
    throw KwiftDBError.io("could not synchronize directory \(directory.path): errno \(errno)")
  }
  #endif
}
