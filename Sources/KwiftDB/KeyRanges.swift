/// Operations on lists of key ranges. A *normalized* list is sorted by start,
/// holds no empty range, and no two of its ranges overlap or touch.
///
/// A sync session holds one range for every key or span its queries read, so
/// these lists grow to thousands of ranges; everything here is linear in the
/// list sizes, or logarithmic for a single key.
enum KeyRanges {
  /// `ranges` sorted, with empty ranges dropped and overlapping or touching
  /// ranges merged.
  static func normalized(_ ranges: [KeyRange]) -> [KeyRange] {
    let sorted = ranges.filter { $0.start.lexicographicallyPrecedes($0.end) }
      .sorted { $0.start.lexicographicallyPrecedes($1.start) }
    var result: [KeyRange] = []
    result.reserveCapacity(sorted.count)
    for range in sorted {
      if let last = result.last, !last.end.lexicographicallyPrecedes(range.start) {
        if last.end.lexicographicallyPrecedes(range.end) { result[result.count - 1].end = range.end }
      } else {
        result.append(range)
      }
    }
    return result
  }

  /// The parts of `ranges` that no range of `removed` covers, normalized.
  static func subtract(_ ranges: [KeyRange], _ removed: [KeyRange]) -> [KeyRange] {
    let cuts = normalized(removed)
    var output: [KeyRange] = []
    var first = 0
    for range in normalized(ranges) {
      // Cuts ending at or before this range end before every later range too.
      while first < cuts.count, !range.start.lexicographicallyPrecedes(cuts[first].end) { first += 1 }
      var cursor = range.start
      var index = first
      while index < cuts.count, cuts[index].start.lexicographicallyPrecedes(range.end) {
        let cut = cuts[index]
        if cursor.lexicographicallyPrecedes(cut.start) { output.append(KeyRange(start: cursor, end: cut.start)) }
        if cursor.lexicographicallyPrecedes(cut.end) { cursor = cut.end }
        if !cursor.lexicographicallyPrecedes(range.end) { break }
        index += 1
      }
      if cursor.lexicographicallyPrecedes(range.end) { output.append(KeyRange(start: cursor, end: range.end)) }
    }
    return output
  }

  /// Whether any range of normalized `lhs` overlaps any range of normalized `rhs`.
  static func overlap(_ lhs: [KeyRange], _ rhs: [KeyRange]) -> Bool {
    var left = 0
    var right = 0
    while left < lhs.count, right < rhs.count {
      let a = lhs[left]
      let b = rhs[right]
      // Half-open ranges overlap when each starts before the other ends.
      if a.start.lexicographicallyPrecedes(b.end) && b.start.lexicographicallyPrecedes(a.end) { return true }
      if a.end.lexicographicallyPrecedes(b.end) { left += 1 } else { right += 1 }
    }
    return false
  }

  /// Whether a range of the normalized list `ranges` contains `key`.
  static func contain(_ ranges: [KeyRange], _ key: [UInt8]) -> Bool {
    // The last range starting at or before `key` is the only one that can hold it.
    var low = 0
    var high = ranges.count
    while low < high {
      let middle = (low + high) / 2
      if key.lexicographicallyPrecedes(ranges[middle].start) { high = middle } else { low = middle + 1 }
    }
    return low > 0 && key.lexicographicallyPrecedes(ranges[low - 1].end)
  }
}
