/// `Codable` conformances that let `Value`, `Number` and `Document` appear as
/// properties of `Codable` types.
///
/// Inside `ValueEncoder` / `ValueDecoder` these types never reach the methods
/// below, because the special-type mapping handles them first at every depth.
/// The methods are the fallback for a custom `Encodable` that hands one of
/// these types to an encoder directly, and they make the conformance
/// compile. Any other coder (for example `JSONEncoder`) is unsupported and
/// throws.
private func requireValueEncoder(_ encoder: any Encoder) throws -> InternalValueEncoder {
  guard let valueEncoder = encoder as? InternalValueEncoder else {
    throw EncodingError.invalidValue(
      encoder,
      .init(
        codingPath: encoder.codingPath,
        debugDescription: "This type can only be encoded with ValueEncoder."))
  }
  return valueEncoder
}

private func requireValueDecoder(_ decoder: any Decoder) throws -> InternalValueDecoder {
  guard let valueDecoder = decoder as? InternalValueDecoder else {
    throw DecodingError.dataCorrupted(
      .init(
        codingPath: decoder.codingPath,
        debugDescription: "This type can only be decoded with ValueDecoder."))
  }
  return valueDecoder
}

extension Value: Codable {
  /// Reads the value this decoder is positioned on, unchanged.
  public init(from decoder: any Decoder) throws {
    self = try requireValueDecoder(decoder).value
  }

  /// Writes this value, unchanged, into the encoder's slot.
  public func encode(to encoder: any Encoder) throws {
    try requireValueEncoder(encoder).node.setLeaf(self)
  }
}

extension Number: Codable {
  /// Reads a `.number`; any other case throws `typeMismatch`.
  public init(from decoder: any Decoder) throws {
    let state = try requireValueDecoder(decoder)
    guard case .number(let number) = state.value else {
      throw CodingErrors.typeMismatch(Number.self, found: state.value, codingPath: state.codingPath)
    }
    self = number
  }

  /// Writes this number as `.number`.
  public func encode(to encoder: any Encoder) throws {
    try requireValueEncoder(encoder).node.setLeaf(.number(self))
  }
}

extension Document: Codable {
  /// Reads a `.document`; any other case throws `typeMismatch`.
  public init(from decoder: any Decoder) throws {
    let state = try requireValueDecoder(decoder)
    guard case .document(let document) = state.value else {
      throw CodingErrors.typeMismatch(
        Document.self, found: state.value, codingPath: state.codingPath)
    }
    self = document
  }

  /// Writes this document as `.document`.
  public func encode(to encoder: any Encoder) throws {
    try requireValueEncoder(encoder).node.setLeaf(.document(self))
  }
}
