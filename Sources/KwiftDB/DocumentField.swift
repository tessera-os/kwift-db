/// One named value in a document.
///
/// A document stores these fields in their declared order. Field order is
/// retained by the value codec, so it remains stable across a write and read.
public struct DocumentField: Sendable, Equatable {
  /// The unique name used to address this field in its document.
  public var name: String
  /// The value held by this field.
  public var value: Value

  /// Creates a field from its name and value.
  ///
  /// Uniqueness is checked by `Document`, where fields are assembled together.
  public init(_ name: String, _ value: Value) {
    self.name = name
    self.value = value
  }
}
