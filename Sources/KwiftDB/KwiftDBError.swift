import Foundation

/// Failures reported by KwiftDB's value, schema, table, and storage layers.
///
/// These errors are deliberately typed so callers can distinguish invalid
/// input from bytes already persisted by a previous process. I/O details are
/// kept as text because the underlying Foundation failure can vary by platform.
public enum KwiftDBError: Error, Sendable, Equatable, LocalizedError {
  /// A decimal string could not be parsed as a finite exact number.
  case invalidNumber(String)
  /// A document contains a duplicate field name or another invalid shape.
  case invalidDocument(String)
  /// Binary value bytes are truncated, malformed, or contain an unknown tag.
  case invalidEncoding(String)
  /// A value type is not allowed in an order-preserving database key.
  case unsupportedKeyValue(String)
  /// A persisted log, table, manifest, or catalog record violates its format.
  case corruptStorage(String)
  /// Foundation could not complete an operation on the database directory.
  case io(String)
  /// A table schema fails one of the schema rules in the database specification.
  case invalidSchema(String)
  /// A row does not satisfy its table schema.
  case invalidRow(String)
  /// A table operation cannot be completed, such as a missing table or key.
  case table(String)

  /// A concise description suitable for logs or an API error response.
  public var errorDescription: String? {
    switch self {
    case .invalidNumber(let reason): "Invalid number: \(reason)"
    case .invalidDocument(let reason): "Invalid document: \(reason)"
    case .invalidEncoding(let reason): "Invalid encoded value: \(reason)"
    case .unsupportedKeyValue(let reason): "Value cannot be encoded as a key: \(reason)"
    case .corruptStorage(let reason): "Corrupt database storage: \(reason)"
    case .io(let reason): "Database I/O failed: \(reason)"
    case .invalidSchema(let reason): "Invalid table schema: \(reason)"
    case .invalidRow(let reason): "Invalid table row: \(reason)"
    case .table(let reason): reason
    }
  }
}
