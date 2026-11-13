/// The value families accepted by a table column.
///
/// `Null` is represented as an absent field for optional columns. Streams are
/// references to separately managed content and are not valid table cells.
public enum ColumnType: String, Sendable, CaseIterable {
  /// A Boolean value.
  case bool = "Bool"
  /// An exact decimal `Number`.
  case number = "Number"
  /// A UTF-8 string.
  case text = "Text"
  /// Opaque bytes.
  case bytes = "Bytes"
  /// A recursively typed value list.
  case list = "List"
  /// A nested document.
  case document = "Document"
  /// An immutable reference to content held by the durable stream layer.
  case stream = "Stream"

  /// Whether values of this type have a stable order for keys and indexes.
  public var isOrderable: Bool {
    switch self {
    case .bool, .number, .text, .bytes: true
    case .list, .document, .stream: false
    }
  }

  internal func accepts(_ value: Value) -> Bool {
    switch (self, value) {
    case (.bool, .bool), (.number, .number), (.text, .text), (.bytes, .bytes),
         (.list, .list), (.document, .document), (.stream, .stream): true
    default: false
    }
  }
}

/// One named column and its nullability rule.
public struct Column: Sendable, Equatable {
  /// A stable schema name, unique within one table.
  public var name: String
  /// The required `Value` case for a non-null cell.
  public var type: ColumnType
  /// Whether a row may omit this field or store it as `Null`.
  public var optional: Bool

  /// Declares a table column.
  public init(name: String, type: ColumnType, optional: Bool = false) {
    self.name = name
    self.type = type
    self.optional = optional
  }
}

/// One secondary index declared by a table schema.
public struct IndexSchema: Sendable, Equatable {
  /// A stable index name, unique within one table.
  public var name: String
  /// Ordered columns that form the index key.
  public var columns: [String]
  /// Whether non-null index tuples must be unique across rows.
  public var unique: Bool

  /// Declares an index.
  public init(name: String, columns: [String], unique: Bool = false) {
    self.name = name
    self.columns = columns
    self.unique = unique
  }
}

/// A validated description of a table's columns, primary key, and indexes.
public struct TableSchema: Sendable, Equatable {
  /// The catalog name used to find this table.
  public var name: String
  /// Columns in stored row order.
  public var columns: [Column]
  /// One or more required, orderable columns that uniquely identify each row.
  public var primaryKey: [String]
  /// Secondary indexes in declaration order.
  public var indexes: [IndexSchema]

  /// Creates a schema after checking every structural constraint.
  public init(
    name: String,
    columns: [Column],
    primaryKey: [String],
    indexes: [IndexSchema] = []
  ) throws {
    self.name = name
    self.columns = columns
    self.primaryKey = primaryKey
    self.indexes = indexes
    try validate()
  }

  /// Checks names, keys, and index definitions against the storage contract.
  public func validate() throws {
    guard !name.isEmpty else { throw KwiftDBError.invalidSchema("table name is empty") }
    var byName: [String: Column] = [:]
    for column in columns {
      guard !column.name.isEmpty else { throw KwiftDBError.invalidSchema("column name is empty") }
      guard byName.updateValue(column, forKey: column.name) == nil else {
        throw KwiftDBError.invalidSchema("duplicate column \(column.name)")
      }
    }
    try Self.validateUniqueNames(primaryKey, label: "primary key column")
    guard !primaryKey.isEmpty else { throw KwiftDBError.invalidSchema("primary key has no columns") }
    for name in primaryKey {
      guard let column = byName[name] else { throw KwiftDBError.invalidSchema("unknown primary key column \(name)") }
      guard !column.optional else { throw KwiftDBError.invalidSchema("primary key column \(name) is optional") }
      guard column.type.isOrderable else { throw KwiftDBError.invalidSchema("primary key column \(name) is not orderable") }
    }
    try Self.validateUniqueNames(indexes.map(\.name), label: "index")
    for index in indexes {
      guard !index.name.isEmpty else { throw KwiftDBError.invalidSchema("index name is empty") }
      guard !index.columns.isEmpty else { throw KwiftDBError.invalidSchema("index \(index.name) has no columns") }
      try Self.validateUniqueNames(index.columns, label: "column in index \(index.name)")
      for name in index.columns {
        guard let column = byName[name] else { throw KwiftDBError.invalidSchema("unknown index column \(name)") }
        guard column.type.isOrderable else { throw KwiftDBError.invalidSchema("index column \(name) is not orderable") }
      }
    }
  }

  internal var columnsByName: [String: Column] { Dictionary(uniqueKeysWithValues: columns.map { ($0.name, $0) }) }

  private static func validateUniqueNames(_ names: [String], label: String) throws {
    var seen = Set<String>()
    for name in names where !seen.insert(name).inserted {
      throw KwiftDBError.invalidSchema("duplicate \(label) \(name)")
    }
  }
}

/// A schema paired with the durable identifiers assigned by its catalog.
public struct Table: Sendable, Equatable {
  /// The validated public schema.
  public var schema: TableSchema
  /// The table's never-reused database id.
  public var id: Number
  /// Index ids corresponding to `schema.indexes` in declaration order.
  public var indexIDs: [Number]

  /// Creates a catalog table value.
  public init(schema: TableSchema, id: Number, indexIDs: [Number]) {
    self.schema = schema
    self.id = id
    self.indexIDs = indexIDs
  }
}

/// A comparison operator used by a table query.
public enum Relation: Sendable, Equatable {
  /// Values have the same type and sortable value.
  case equal
  /// Values differ.
  case notEqual
  /// The row value sorts before the comparison value.
  case less
  /// The row value sorts before or equals the comparison value.
  case lessOrEqual
  /// The row value sorts after the comparison value.
  case greater
  /// The row value sorts after or equals the comparison value.
  case greaterOrEqual
}

/// One typed column comparison.
public struct Comparison: Sendable {
  /// The column to inspect.
  public var column: String
  /// The comparison operation.
  public var relation: Relation
  /// A non-null value matching the column's declared type.
  public var value: Value

  /// Creates a comparison predicate.
  public init(column: String, relation: Relation, value: Value) {
    self.column = column
    self.relation = relation
    self.value = value
  }
}

/// A composable row condition.
public indirect enum Condition: Sendable {
  /// Compares a present, orderable cell.
  case compare(Comparison)
  /// Matches absent or `Null` cells in an optional column.
  case isNull(String)
  /// Requires all child conditions; an empty list matches every row.
  case allOf([Condition])
  /// Requires at least one child condition; an empty list matches no row.
  case anyOf([Condition])
}

/// The direction of one requested sort column.
public enum Direction: Sendable {
  /// Smallest sortable value first.
  case ascending
  /// Largest sortable value first.
  case descending
}

/// One requested result ordering.
public struct Order: Sendable {
  /// The orderable table column to sort by.
  public var column: String
  /// The direction for this column.
  public var direction: Direction

  /// Creates an order term.
  public init(column: String, direction: Direction = .ascending) {
    self.column = column
    self.direction = direction
  }
}

/// A table query with conditions, ordering, limit, and optional projection.
public struct Query: Sendable {
  /// The table to query.
  public var table: Table
  /// Every condition that a returned row must satisfy.
  public var conditions: [Condition]
  /// Sort terms applied in order; primary key order breaks ties.
  public var orders: [Order]
  /// Maximum number of rows, or `nil` for no limit.
  public var limit: Int?
  /// Requested output columns; an empty list returns the full row.
  public var columns: [String]

  /// Creates a query over a specific catalog table.
  public init(
    table: Table,
    conditions: [Condition] = [],
    orders: [Order] = [],
    limit: Int? = nil,
    columns: [String] = []
  ) {
    self.table = table
    self.conditions = conditions
    self.orders = orders
    self.limit = limit
    self.columns = columns
  }
}
