import Foundation

/// Computes the Castagnoli CRC used to detect torn or damaged storage records.
///
/// KwiftDB writes this checksum into every WAL record and sorted-table block.
/// The checksum is over the payload only; its four-byte file representation is
/// emitted most-significant byte first by the corresponding storage encoder.
internal enum CRC32C {
  /// Returns the CRC-32C checksum of `bytes`.
  ///
  /// Slicing-by-8: eight bytes per step through eight precomputed tables,
  /// reading the bytes in place.
  static func checksum(_ bytes: some ContiguousBytes) -> UInt32 {
    bytes.withUnsafeBytes { buffer in
      tables.withUnsafeBufferPointer { table in
        var crc: UInt32 = 0xffff_ffff
        var index = 0
        let count = buffer.count
        while index + 8 <= count {
          let low = crc ^ (UInt32(buffer[index]) | UInt32(buffer[index + 1]) << 8
            | UInt32(buffer[index + 2]) << 16 | UInt32(buffer[index + 3]) << 24)
          let high = UInt32(buffer[index + 4]) | UInt32(buffer[index + 5]) << 8
            | UInt32(buffer[index + 6]) << 16 | UInt32(buffer[index + 7]) << 24
          crc = table[7 &* 256 &+ Int(low & 0xff)] ^ table[6 &* 256 &+ Int((low >> 8) & 0xff)]
            ^ table[5 &* 256 &+ Int((low >> 16) & 0xff)] ^ table[4 &* 256 &+ Int(low >> 24)]
            ^ table[3 &* 256 &+ Int(high & 0xff)] ^ table[2 &* 256 &+ Int((high >> 8) & 0xff)]
            ^ table[1 &* 256 &+ Int((high >> 16) & 0xff)] ^ table[Int(high >> 24)]
          index &+= 8
        }
        while index < count {
          crc = table[Int((crc ^ UInt32(buffer[index])) & 0xff)] ^ (crc >> 8)
          index &+= 1
        }
        return ~crc
      }
    }
  }

  /// Table `k` (entries `256k ..< 256(k+1)`) advances a byte by `k` more
  /// bytes of zeros; table 0 is the classic byte table.
  private static let tables: [UInt32] = {
    var tables = [UInt32](repeating: 0, count: 8 * 256)
    for byte in 0..<256 {
      var crc = UInt32(byte)
      for _ in 0..<8 { crc = (crc & 1) == 1 ? (crc >> 1) ^ 0x82f6_3b78 : crc >> 1 }
      tables[byte] = crc
    }
    for byte in 0..<256 {
      for slice in 1..<8 {
        let previous = tables[(slice - 1) * 256 + byte]
        tables[slice * 256 + byte] = tables[Int(previous & 0xff)] ^ (previous >> 8)
      }
    }
    return tables
  }()

  /// Appends a checksum as a four-byte big-endian word.
  static func append(_ checksum: UInt32, to data: inout Data) {
    data.append(UInt8((checksum >> 24) & 0xff))
    data.append(UInt8((checksum >> 16) & 0xff))
    data.append(UInt8((checksum >> 8) & 0xff))
    data.append(UInt8(checksum & 0xff))
  }
}
