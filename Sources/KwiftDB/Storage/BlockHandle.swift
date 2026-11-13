/// The location and final key of one data block in a sorted table.
internal struct BlockHandle: Sendable, Equatable {
  /// The last key in this block, inclusive.
  var lastKey: [UInt8]
  /// The byte offset from the start of the table file.
  var offset: UInt64
  /// The data-block byte length, excluding the four-byte checksum.
  var length: Int
}
