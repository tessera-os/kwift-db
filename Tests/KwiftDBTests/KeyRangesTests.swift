@testable import KwiftDB
import Testing

@Suite("Key ranges")
struct KeyRangesTests {
  private func range(_ start: UInt8, _ end: UInt8) -> KeyRange { KeyRange(start: [start], end: [end]) }

  @Test func normalizedMergesOverlappingAndTouching() {
    let merged = KeyRanges.normalized([range(5, 7), range(1, 3), range(3, 4), range(6, 9), range(8, 8)])
    #expect(merged == [range(1, 4), range(5, 9)])
  }

  @Test func subtractCutsEveryRange() {
    let left = KeyRanges.subtract([range(0, 10), range(20, 30)], [range(2, 4), range(8, 22), range(25, 26)])
    #expect(left == [range(0, 2), range(4, 8), range(22, 25), range(26, 30)])
  }

  @Test func subtractKeepsUncoveredAndDropsCovered() {
    #expect(KeyRanges.subtract([range(1, 3)], []) == [range(1, 3)])
    #expect(KeyRanges.subtract([range(1, 3)], [range(0, 5)]).isEmpty)
    #expect(KeyRanges.subtract([range(1, 3), range(6, 8)], [range(3, 6)]) == [range(1, 3), range(6, 8)])
  }

  /// The sweep agrees with cutting each range by each removed range in turn.
  @Test func subtractMatchesDirectCutting() {
    var generator = SystemRandomNumberGenerator()
    for _ in 0..<200 {
      let ranges = (0..<8).map { _ in randomRange(using: &generator) }
      let removed = (0..<8).map { _ in randomRange(using: &generator) }
      let expected = KeyRanges.normalized(ranges.flatMap { piece in
        removed.reduce([piece]) { pieces, cut in pieces.flatMap { cutOne($0, by: cut) } }
      })
      #expect(KeyRanges.normalized(KeyRanges.subtract(ranges, removed)) == expected)
    }
  }

  @Test func containFindsKeysByRange() {
    let ranges = KeyRanges.normalized([range(1, 3), range(5, 7), range(10, 12)])
    let held = (0..<14).filter { KeyRanges.contain(ranges, [UInt8($0)]) }
    #expect(held == [1, 2, 5, 6, 10, 11])
    #expect(!KeyRanges.contain([], [1]))
  }

  private func randomRange(using generator: inout some RandomNumberGenerator) -> KeyRange {
    let start = UInt8.random(in: 0..<40, using: &generator)
    return range(start, start + UInt8.random(in: 0..<10, using: &generator))
  }

  private func cutOne(_ piece: KeyRange, by cut: KeyRange) -> [KeyRange] {
    guard cut.start.lexicographicallyPrecedes(cut.end) else { return [piece] }
    var pieces: [KeyRange] = []
    if piece.start.lexicographicallyPrecedes(cut.start) {
      pieces.append(KeyRange(start: piece.start, end: earlier(piece.end, cut.start)))
    }
    if cut.end.lexicographicallyPrecedes(piece.end) {
      pieces.append(KeyRange(start: later(piece.start, cut.end), end: piece.end))
    }
    return pieces.filter { $0.start.lexicographicallyPrecedes($0.end) }
  }

  private func earlier(_ lhs: [UInt8], _ rhs: [UInt8]) -> [UInt8] {
    lhs.lexicographicallyPrecedes(rhs) ? lhs : rhs
  }

  private func later(_ lhs: [UInt8], _ rhs: [UInt8]) -> [UInt8] {
    lhs.lexicographicallyPrecedes(rhs) ? rhs : lhs
  }
}
