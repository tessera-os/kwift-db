import Foundation
import Testing

@testable import KwiftDB

// MARK: - Fixtures

private struct Address: Codable, Equatable {
  var street: String
  var zip: Int
}

private struct Person: Codable, Equatable {
  var name: String
  var age: Int
  var address: Address
  var tags: [String]
  var nickname: String?
}

private enum Color: String, Codable { case red, green }
private enum Level: Int, Codable { case low = 1, high = 9 }
private enum Shape: Codable, Equatable {
  case circle(radius: Double)
  case rect(width: Int, height: Int)
  case unit
}

private struct Palette: Codable, Equatable {
  var color: Color
  var level: Level
  var shapes: [Shape]
}

private struct Stamp: Codable, Equatable {
  var when: Date
  var id: UUID
  var link: URL
  var blob: Data
}

private struct Passthrough: Codable, Equatable {
  var raw: Value
  var number: Number
  var doc: Document
}

private struct Ordered: Codable, Equatable {
  var z: Int
  var a: Int
  var m: Int
}

private struct Item: Codable, Equatable { var count: Int }
private struct Basket: Codable, Equatable { var items: [Item] }
private struct FloatBox: Codable, Equatable { var x: Double }

private struct Inner: Encodable {
  func encode(to encoder: any Encoder) throws {
    var c = encoder.container(keyedBy: ValueCodingKey.self)
    try c.encode(1, forKey: ValueCodingKey(stringValue: "one"))
  }
}

private struct Nested: Encodable {
  func encode(to encoder: any Encoder) throws {
    var c = encoder.container(keyedBy: ValueCodingKey.self)
    try c.encode("first", forKey: ValueCodingKey(stringValue: "a"))
    var inner = c.nestedContainer(keyedBy: ValueCodingKey.self, forKey: ValueCodingKey(stringValue: "b"))
    try inner.encode(true, forKey: ValueCodingKey(stringValue: "x"))
    var list = c.nestedUnkeyedContainer(forKey: ValueCodingKey(stringValue: "c"))
    try list.encode(1)
    try list.encode(2)
    try Inner().encode(to: c.superEncoder())
    try Inner().encode(to: c.superEncoder(forKey: ValueCodingKey(stringValue: "custom")))
  }
}

private struct NullWriter: Codable, Equatable {
  var value: String?
  func encode(to encoder: any Encoder) throws {
    var c = encoder.container(keyedBy: ValueCodingKey.self)
    try c.encodeNil(forKey: ValueCodingKey(stringValue: "value"))
  }
  init(value: String?) { self.value = value }
  init(from decoder: any Decoder) throws {
    let c = try decoder.container(keyedBy: ValueCodingKey.self)
    value = try c.decodeIfPresent(String.self, forKey: ValueCodingKey(stringValue: "value"))
  }
}

private struct OptionalHolder: Codable, Equatable {
  var maybe: Int?
}

private func number(_ text: String) -> Value { .number(try! Number(text)) }

private func decodingPath(_ error: any Error) -> [String]? {
  guard let error = error as? DecodingError else { return nil }
  switch error {
  case .typeMismatch(_, let c), .valueNotFound(_, let c), .keyNotFound(_, let c), .dataCorrupted(let c):
    return c.codingPath.map(\.stringValue)
  @unknown default: return nil
  }
}

private func isDataCorrupted(_ error: any Error) -> Bool {
  if case DecodingError.dataCorrupted = error { return true }
  return false
}

private func roundTrip<T: Codable & Equatable>(_ value: T) throws -> T {
  let encoded = try ValueEncoder().encode(value)
  return try ValueDecoder().decode(T.self, from: encoded)
}

// MARK: - Tests

@Suite("Value coder")
struct ValueCodableTests {
  @Test func nestedStructRoundTripsAndMapsToDocument() throws {
    let person = Person(
      name: "Ada", age: 36, address: Address(street: "Main", zip: 12345),
      tags: ["a", "b"], nickname: "A")
    let value = try ValueEncoder().encode(person)
    let expected = Value.document(
      try Document(fields: [
        DocumentField("name", .text("Ada")),
        DocumentField("age", number("36")),
        DocumentField(
          "address",
          .document(
            try Document(fields: [
              DocumentField("street", .text("Main")), DocumentField("zip", number("12345")),
            ]))),
        DocumentField("tags", .list([.text("a"), .text("b")])),
        DocumentField("nickname", .text("A")),
      ]))
    #expect(value == expected)
    #expect(try roundTrip(person) == person)
  }

  @Test func absentVersusNullOptionals() throws {
    let none = OptionalHolder(maybe: nil)
    let encoded = try ValueEncoder().encode(none)
    #expect(encoded == .document(try Document()))
    #expect(try ValueDecoder().decode(OptionalHolder.self, from: encoded) == none)

    let nullDoc = Value.document(try Document(fields: [DocumentField("maybe", .null)]))
    #expect(try ValueDecoder().decode(OptionalHolder.self, from: nullDoc) == none)

    // An explicit encodeNil writes .null, and decodeIfPresent reads it as nil.
    let written = try ValueEncoder().encode(NullWriter(value: nil))
    #expect(written == nullDoc.replacingField("maybe", "value"))
    #expect(try ValueDecoder().decode(NullWriter.self, from: written).value == nil)
  }

  @Test func decodeNilIsTrueForAbsentAndNull() throws {
    struct Probe: Decodable {
      let absent: Bool
      let null: Bool
      let present: Bool
      init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: ValueCodingKey.self)
        absent = try c.decodeNil(forKey: ValueCodingKey(stringValue: "nope"))
        null = try c.decodeNil(forKey: ValueCodingKey(stringValue: "n"))
        present = try c.decodeNil(forKey: ValueCodingKey(stringValue: "p"))
      }
    }
    let doc = try Document(fields: [DocumentField("n", .null), DocumentField("p", .bool(true))])
    let probe = try doc.decode(Probe.self)
    #expect(probe.absent && probe.null && !probe.present)
  }

  @Test func enumsRoundTrip() throws {
    let palette = Palette(
      color: .green, level: .high,
      shapes: [.circle(radius: 1.5), .rect(width: 2, height: 3), .unit])
    #expect(try roundTrip(palette) == palette)
    let value = try ValueEncoder().encode(palette)
    guard case .document(let doc) = value else { Issue.record("not a document"); return }
    #expect(doc["color"] == .text("green"))
    #expect(doc["level"] == number("9"))
  }

  @Test func arraysAndDictionaries() throws {
    #expect(try ValueEncoder().encode([1, 2, 3]) == .list([number("1"), number("2"), number("3")]))
    #expect(try roundTrip([[1, 2], [], [3]]) == [[1, 2], [], [3]])

    let dict = ["b": 2, "a": 1]
    let encoded = try ValueEncoder().encode(dict)
    guard case .document(let doc) = encoded else { Issue.record("not a document"); return }
    #expect(doc["a"] == number("1") && doc["b"] == number("2"))
    #expect(try roundTrip(dict) == dict)

    // Swift's own Dictionary conformance encodes Int keys as keyed fields (decimal text).
    let intKeyed = [1: "x", 2: "y"]
    #expect(try ValueEncoder().encode(intKeyed).documentValue?["1"] == .text("x"))
    #expect(try roundTrip(intKeyed) == intKeyed)

    // Other key types are encoded by Swift as alternating key/value elements.
    let boolKeyed = [true: 5]
    #expect(try ValueEncoder().encode(boolKeyed) == .list([.bool(true), number("5")]))
    #expect(try roundTrip(boolKeyed) == boolKeyed)
  }

  @Test func foundationTypes() throws {
    let stamp = Stamp(
      when: Date(timeIntervalSince1970: 1_700_000_000.5),
      id: UUID(uuidString: "E621E1F8-C36C-495A-93FC-0C247A3E6E5F")!,
      link: URL(string: "https://example.com/a?b=c")!,
      blob: Data([0, 1, 2, 255]))
    let doc = try Document(encoding: stamp)
    #expect(doc["when"] == number("1700000000500"))
    #expect(doc["id"] == .text("e621e1f8-c36c-495a-93fc-0c247a3e6e5f"))
    #expect(doc["link"] == .text("https://example.com/a?b=c"))
    #expect(doc["blob"] == .bytes([0, 1, 2, 255]))
    #expect(try doc.decode(Stamp.self) == stamp)
  }

  @Test func specialTypesAtDepth() throws {
    let dates = [Date(timeIntervalSince1970: 1), Date(timeIntervalSince1970: -2)]
    #expect(try ValueEncoder().encode(dates) == .list([number("1000"), number("-2000")]))
    #expect(try roundTrip(["k": [Data([9])]]) == ["k": [Data([9])]])
  }

  @Test func dateDecodeRoundsTowardZero() throws {
    let positive = try ValueDecoder().decode(Date.self, from: number("1500.9"))
    #expect(positive.timeIntervalSince1970 == 1.5)
    let negative = try ValueDecoder().decode(Date.self, from: number("-1500.9"))
    #expect(negative.timeIntervalSince1970 == -1.5)
  }

  @Test func badUUIDAndURLAreDataCorrupted() {
    #expect(throws: DecodingError.self) {
      try ValueDecoder().decode(UUID.self, from: .text("nope"))
    }
    #expect(throws: DecodingError.self) {
      try ValueDecoder().decode(Data.self, from: .text("nope"))
    }
  }

  @Test func passthroughTypes() throws {
    let doc = try Document(fields: [DocumentField("k", .list([.null, .bool(true)]))])
    let holder = Passthrough(raw: .stream("s-1"), number: try Number("1.25e3"), doc: doc)
    let encoded = try ValueEncoder().encode(holder)
    guard case .document(let fields) = encoded else { Issue.record("not a document"); return }
    #expect(fields["raw"] == .stream("s-1"))
    #expect(fields["number"] == .number(try Number("1250")))
    #expect(fields["doc"] == .document(doc))
    #expect(try roundTrip(holder) == holder)
    #expect(try ValueEncoder().encode(Value.text("x")) == .text("x"))
    #expect(try Document(encoding: doc) == doc)
  }

  @Test func foreignCoderIsUnsupported() {
    #expect(throws: (any Error).self) { try JSONEncoder().encode(Value.null) }
  }

  @Test func integerExactnessAndRange() throws {
    #expect(try roundTrip(Int64.max) == Int64.max)
    #expect(try roundTrip(Int64.min) == Int64.min)
    #expect(try roundTrip(UInt64.max) == UInt64.max)
    #expect(try roundTrip(Int8.min) == Int8.min)
    #expect(try ValueDecoder().decode(Int.self, from: number("12.0")) == 12)
    #expect(try ValueDecoder().decode(Int.self, from: number("1.2e2")) == 120)

    #expect(throws: DecodingError.self) { try ValueDecoder().decode(Int.self, from: number("1.5")) }
    #expect(throws: DecodingError.self) { try ValueDecoder().decode(Int.self, from: number("0.001")) }
    #expect(throws: DecodingError.self) { try ValueDecoder().decode(UInt8.self, from: number("256")) }
    #expect(throws: DecodingError.self) { try ValueDecoder().decode(Int8.self, from: number("-129")) }
    #expect(throws: DecodingError.self) { try ValueDecoder().decode(UInt.self, from: number("-1")) }
    #expect(throws: DecodingError.self) {
      try ValueDecoder().decode(Int64.self, from: number("9223372036854775808"))
    }
    #expect(throws: DecodingError.self) { try ValueDecoder().decode(Int.self, from: number("1e400")) }
    do {
      _ = try ValueDecoder().decode(Int.self, from: number("1.5"))
    } catch {
      #expect(isDataCorrupted(error))
    }
  }

  @Test func floatingPoint() throws {
    #expect(try ValueEncoder().encode(0.1) == number("0.1"))
    #expect(try ValueEncoder().encode(Float(0.1)) == number("0.1"))
    for sample in [0.1, 1e22, -2.5e-300, Double.greatestFiniteMagnitude, 5e-324, 123456.789] {
      #expect(try roundTrip(sample) == sample)
    }
    #expect(try roundTrip(Float.greatestFiniteMagnitude) == Float.greatestFiniteMagnitude)
    #expect(throws: DecodingError.self) { try ValueDecoder().decode(Double.self, from: number("1e400")) }
  }

  @Test func nonFiniteThrowsEncodingError() {
    for bad in [Double.nan, .infinity, -.infinity] {
      #expect(throws: EncodingError.self) { try ValueEncoder().encode(bad) }
    }
    #expect(throws: EncodingError.self) { try ValueEncoder().encode(Float.nan) }
    do {
      _ = try ValueEncoder().encode(FloatBox(x: .nan))
    } catch EncodingError.invalidValue(_, let context) {
      #expect(context.codingPath.map(\.stringValue) == ["x"])
    } catch {
      Issue.record("unexpected error \(error)")
    }
  }

  @Test func fieldOrderIsPreserved() throws {
    let doc = try Document(encoding: Ordered(z: 1, a: 2, m: 3))
    #expect(doc.fields.map(\.name) == ["z", "a", "m"])
  }

  @Test func superAndNestedContainers() throws {
    let doc = try Document(encoding: Nested())
    #expect(doc.fields.map(\.name) == ["a", "b", "c", "super", "custom"])
    #expect(doc["b"]?.documentValue?["x"] == .bool(true))
    #expect(doc["c"] == .list([number("1"), number("2")]))
    #expect(doc["super"]?.documentValue?["one"] == number("1"))
    #expect(doc["custom"]?.documentValue?["one"] == number("1"))
  }

  @Test func topLevelMustBeDocument() {
    #expect(throws: EncodingError.self) { try Document(encoding: 5) }
    #expect(throws: EncodingError.self) { try Document(encoding: [1, 2]) }
  }

  @Test func codingPathsInDecodingErrors() throws {
    let value = Value.document(
      try Document(fields: [
        DocumentField(
          "items",
          .list([
            .document(try Document(fields: [DocumentField("count", number("1"))])),
            .document(try Document(fields: [DocumentField("count", number("2.5"))])),
          ]))
      ]))
    do {
      _ = try ValueDecoder().decode(Basket.self, from: value)
      Issue.record("expected failure")
    } catch {
      #expect(isDataCorrupted(error))
      #expect(decodingPath(error) == ["items", "Index 1", "count"])
    }
  }

  @Test func typeMismatchNamesFoundCase() throws {
    let value = Value.document(try Document(fields: [DocumentField("zip", .text("x")), DocumentField("street", .text("s"))]))
    do {
      _ = try ValueDecoder().decode(Address.self, from: value)
      Issue.record("expected failure")
    } catch DecodingError.typeMismatch(_, let context) {
      #expect(context.debugDescription.contains("found text"))
      #expect(context.codingPath.map(\.stringValue) == ["zip"])
    } catch {
      Issue.record("unexpected error \(error)")
    }
    #expect(throws: DecodingError.self) { try ValueDecoder().decode(Address.self, from: .list([])) }
  }

  @Test func missingKeyAndListExhaustion() throws {
    let doc = try Document(fields: [DocumentField("street", .text("s"))])
    do {
      _ = try doc.decode(Address.self)
      Issue.record("expected failure")
    } catch DecodingError.keyNotFound(let key, let context) {
      #expect(key.stringValue == "zip")
      #expect(context.codingPath.isEmpty)
    } catch {
      Issue.record("unexpected error \(error)")
    }
    #expect(throws: DecodingError.self) {
      try ValueDecoder().decode([Int].self, from: .list([number("1"), .text("x")]))
    }
    struct Pair: Decodable {
      init(from decoder: any Decoder) throws {
        var c = try decoder.unkeyedContainer()
        _ = try c.decode(Int.self)
        _ = try c.decode(Int.self)
      }
    }
    do {
      _ = try ValueDecoder().decode(Pair.self, from: .list([number("1")]))
      Issue.record("expected failure")
    } catch DecodingError.valueNotFound(_, let context) {
      #expect(context.codingPath.map(\.stringValue) == ["Index 1"])
    } catch {
      Issue.record("unexpected error \(error)")
    }
  }

  @Test func userInfoReachesTypes() throws {
    struct Key { static let key = CodingUserInfoKey(rawValue: "k")! }
    struct Reader: Decodable {
      let seen: Bool
      init(from decoder: any Decoder) throws { seen = decoder.userInfo[Key.key] as? Int == 7 }
    }
    var decoder = ValueDecoder()
    decoder.userInfo[Key.key] = 7
    #expect(try decoder.decode(Reader.self, from: .null).seen)
  }
}

extension Value {
  fileprivate var documentValue: Document? {
    if case .document(let document) = self { return document }
    return nil
  }

  /// Renames a top-level field of a document value (test helper).
  fileprivate func replacingField(_ old: String, _ new: String) -> Value {
    guard case .document(let doc) = self else { return self }
    let fields = doc.fields.map { $0.name == old ? DocumentField(new, $0.value) : $0 }
    return .document(try! Document(fields: fields))
  }
}
