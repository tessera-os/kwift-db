/// An ordered collection of uniquely named values.
///
/// Documents are the durable record format for KwiftDB's document face and the
/// schema/catalog format for its table face. The field array is ordered, not a
/// dictionary, because the binary codec and table schema both depend on a
/// stable field sequence.
public struct Document: Sendable, Equatable, Sequence {
  /// Fields in their stored order.
  public private(set) var fields: [DocumentField]

  /// Creates a document and rejects duplicate field names.
  ///
  /// - Parameter fields: The ordered fields to store.
  /// - Throws: `KwiftDBError.invalidDocument` if any name occurs more than once.
  public init(fields: [DocumentField] = []) throws {
    var names = Set<String>()
    for field in fields where !names.insert(field.name).inserted {
      throw KwiftDBError.invalidDocument("field name \(field.name) appears more than once")
    }
    self.fields = fields
  }

  /// Creates a document from any sequence of fields.
  ///
  /// This overload makes it convenient to build a document from a collection
  /// while still validating uniqueness at the document boundary.
  ///
  /// - Parameter fields: The ordered sequence of fields.
  /// - Throws: `KwiftDBError.invalidDocument` when a name is repeated.
  public init<S: Sequence>(_ fields: S) throws where S.Element == DocumentField {
    try self.init(fields: Array(fields))
  }

  /// Returns the value associated with `name`, if that field is present.
  public subscript(_ name: String) -> Value? {
    fields.first(where: { $0.name == name })?.value
  }

  /// Creates an iterator that yields fields in stored order.
  public func makeIterator() -> IndexingIterator<[DocumentField]> {
    fields.makeIterator()
  }

  /// The number of fields in the document.
  public var count: Int { fields.count }
}
