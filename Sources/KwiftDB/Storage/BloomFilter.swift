import Foundation

/// A Bloom filter over a table's distinct keys.
///
/// A negative answer is definitive: the key cannot be in the table. A positive
/// answer means the table's index must be checked. The on-disk representation
/// is varint bit count, varint probe count, then low-bit-first bytes.
internal struct BloomFilter: Sendable {
  private(set) var bits: [UInt8]
  let bitCount: Int
  let probes: Int

  private init(bits: [UInt8], bitCount: Int, probes: Int) {
    self.bits = bits
    self.bitCount = bitCount
    self.probes = probes
  }

  /// Builds the configured filter for all distinct keys in a sorted table.
  static func build(keys: [[UInt8]]) throws -> BloomFilter {
    let requested = keys.count.multipliedReportingOverflow(by: Tuning.filterBitsPerKey)
    guard !requested.overflow else { throw KwiftDBError.io("Bloom filter is too large") }
    let bitCount = max(64, requested.partialValue)
    let byteCount = bitCount / 8 + (bitCount % 8 == 0 ? 0 : 1)
    var filter = BloomFilter(
      bits: Array(repeating: 0, count: byteCount),
      bitCount: bitCount,
      probes: Tuning.filterProbes
    )
    for key in keys { filter.insert(key) }
    return filter
  }

  /// Whether every probe bit for `key` is set.
  func mayContain(_ key: [UInt8]) -> Bool {
    guard bitCount > 0 && probes > 0 else { return false }
    let first = Self.fnv1a(key, startingAt: 2_166_136_261)
    let second = Self.fnv1a(key, startingAt: first)
    for probe in 0..<probes {
      let position = (UInt64(first) + UInt64(probe) * UInt64(second)) % UInt64(bitCount)
      let byte = Int(position / 8)
      let mask = UInt8(1) << UInt8(position % 8)
      if bits[byte] & mask == 0 { return false }
    }
    return true
  }

  /// Serializes the bit count, probe count, and packed bit bytes.
  func encode() -> Data {
    var data = Data()
    BinaryWriter.append(DatabaseUInt(UInt64(bitCount)), to: &data)
    BinaryWriter.append(DatabaseUInt(UInt64(probes)), to: &data)
    data.append(contentsOf: bits)
    return data
  }

  /// Parses a filter block and verifies that its declared bits fit the payload.
  static func decode(_ data: Data) throws -> BloomFilter {
    var reader = BinaryReader(data)
    let rawBitCount = try reader.readVarint()
    let rawProbes = try reader.readVarint()
    guard let bitCount = rawBitCount.intValue, bitCount >= 64,
          let probes = rawProbes.intValue, probes > 0, probes <= bitCount
    else { throw KwiftDBError.corruptStorage("invalid Bloom-filter dimensions") }
    let byteCount = bitCount / 8 + (bitCount % 8 == 0 ? 0 : 1)
    let bits = try reader.readBytes(count: byteCount)
    guard reader.isAtEnd else { throw KwiftDBError.corruptStorage("trailing Bloom-filter bytes") }
    return BloomFilter(bits: bits, bitCount: bitCount, probes: probes)
  }

  /// Sets every probe bit for `key` while constructing a table filter.
  private mutating func insert(_ key: [UInt8]) {
    let first = Self.fnv1a(key, startingAt: 2_166_136_261)
    let second = Self.fnv1a(key, startingAt: first)
    for probe in 0..<probes {
      let position = (UInt64(first) + UInt64(probe) * UInt64(second)) % UInt64(bitCount)
      bits[Int(position / 8)] |= UInt8(1) << UInt8(position % 8)
    }
  }

  /// Runs 32-bit FNV-1a with the wrapping multiplication required by the format.
  private static func fnv1a(_ bytes: [UInt8], startingAt start: UInt32) -> UInt32 {
    bytes.reduce(start) { ($0 ^ UInt32($1)) &* 16_777_619 }
  }
}
