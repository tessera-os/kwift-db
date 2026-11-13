/// A value accepted by KwiftDB's document and table storage layers.
///
/// The cases correspond to the stable value tags in the KiraDB specification.
/// `Document`, `List`, and `Stream` are valid stored values but cannot be used
/// directly as ordered keys. The associated `Number` type preserves decimal
/// precision independently of machine floating-point formats.
public indirect enum Value: Sendable, Equatable {
  /// An explicit absence of a value.
  case null
  /// A Boolean value.
  case bool(Bool)
  /// An exact finite decimal number.
  case number(Number)
  /// UTF-8 text.
  case text(String)
  /// Opaque bytes.
  case bytes([UInt8])
  /// An ordered sequence of values.
  case list([Value])
  /// An ordered set of uniquely named fields.
  case document(Document)
  /// A stable reference to content stored by the durable-stream layer.
  case stream(String)
}
