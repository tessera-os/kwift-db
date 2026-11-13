import Foundation

/// Binary encoding for sorted-table entries, block handles, and the footer.
internal enum TableCodec {
  static let footerBytes = 24
  static let magic = Array("KIRADB01".utf8)

  /// Appends a key version using length-prefixed key and value fields.
  static func append(_ entry: Entry, to data: inout Data) {
    BinaryWriter.appendLengthPrefixed(entry.key, to: &data)
    BinaryWriter.append(entry.sequence, to: &data)
    switch entry.version {
    case .put(let value):
      data.append(1)
      BinaryWriter.appendLengthPrefixed(value, to: &data)
    case .delete:
      data.append(2)
    }
  }

  /// Reads one key version from a data block.
  static func readEntry(from reader: inout BinaryReader) throws -> Entry {
    let key = try reader.readLengthPrefixedBytes()
    let sequence = try reader.readVarint()
    switch try reader.readByte() {
    case 1:
      let value = try reader.readLengthPrefixedBytes()
      return Entry(key: key, sequence: sequence, version: .put(value))
    case 2:
      return Entry(key: key, sequence: sequence, version: .delete)
    case let kind:
      throw KwiftDBError.invalidEncoding("unknown sorted-table version kind \(kind)")
    }
  }

  /// Appends a block's last key, offset, and content length to an index block.
  static func append(_ handle: BlockHandle, to data: inout Data) {
    BinaryWriter.appendLengthPrefixed(handle.lastKey, to: &data)
    BinaryWriter.append(DatabaseUInt(handle.offset), to: &data)
    BinaryWriter.append(DatabaseUInt(UInt64(handle.length)), to: &data)
  }

  /// Reads a block handle and ensures its fields fit this process and file API.
  static func readBlockHandle(from reader: inout BinaryReader) throws -> BlockHandle {
    let key = try reader.readLengthPrefixedBytes()
    let rawOffset = try reader.readVarint()
    let rawLength = try reader.readVarint()
    guard let offset = rawOffset.uint64Value, let length = rawLength.intValue else {
      throw KwiftDBError.invalidEncoding("sorted-table block handle is too large")
    }
    return BlockHandle(lastKey: key, offset: offset, length: length)
  }
}
