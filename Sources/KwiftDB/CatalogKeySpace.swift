/// Byte-prefix translation for server catalogs stored beside a device catalog.
internal enum CatalogKeySpace {
  private static let localCatalogPrefix = (try? KeyEncoder.encode(.text("c"))) ?? []
  private static let remoteCatalogPrefix = (try? KeyEncoder.encode(.text("sync-remote-catalog"))) ?? []

  static func remoteKey(_ key: [UInt8]) -> [UInt8] {
    guard key.starts(with: localCatalogPrefix) else { return key }
    return remoteCatalogPrefix + key.dropFirst(localCatalogPrefix.count)
  }

  static func localKey(_ key: [UInt8]) -> [UInt8] {
    guard key.starts(with: remoteCatalogPrefix) else { return key }
    return localCatalogPrefix + key.dropFirst(remoteCatalogPrefix.count)
  }

  static func remoteRange(_ range: KeyRange) -> KeyRange {
    guard range.start.starts(with: localCatalogPrefix) else { return range }
    let start = remoteKey(range.start)
    let localUpper = upperBound(localCatalogPrefix)
    let end: [UInt8]
    if range.end == localUpper {
      end = upperBound(remoteCatalogPrefix)
    } else {
      end = remoteKey(range.end)
    }
    return KeyRange(start: start, end: end)
  }

  static func upperBound(_ prefix: [UInt8]) -> [UInt8] {
    var result = prefix
    for index in result.indices.reversed() where result[index] != .max {
      result[index] += 1
      result.removeSubrange((index + 1)..<result.count)
      return result
    }
    return prefix + [0]
  }
}
