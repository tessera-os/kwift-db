import Foundation

/// The schema catalog and row operations for a database session.
///
/// Catalog, row, and index keys occupy separate first-component namespaces.
/// Creating and dropping tables write their catalog changes in the same batch
/// as row/index data changes. A `TableCatalog` may wrap a live database or a
/// `RecordedRun`, allowing table functions to participate in sync tracking.
public actor TableCatalog {
  private let session: any DatabaseSession

  /// Creates a table API for a database or recorded run.
  public init(session: any DatabaseSession) { self.session = session }

  /// Finds a table by its schema name.
  public func table(named name: String) async throws -> Table? {
    guard let value = try await readValue(Self.catalogKey(name)) else { return nil }
    return try Self.table(from: value, name: name)
  }

  /// Creates a table and allocates its table and index ids atomically.
  public func create(_ schema: TableSchema) async throws -> Table {
    try schema.validate()
    guard try await readValue(Self.catalogKey(schema.name)) == nil else {
      throw KwiftDBError.table("table already exists: \(schema.name)")
    }
    let nextKey = try KeyEncoder.encodeTuple([.text("c"), .text("next-id")])
    let nextValue = try await readValue(nextKey) ?? .number(try Number("1"))
    var nextID = try Self.unsignedInteger(nextValue)
    let tableID = nextID
    nextID = nextID.incremented()
    var indexIDs: [DatabaseUInt] = []
    for _ in schema.indexes {
      indexIDs.append(nextID)
      nextID = nextID.incremented()
    }
    let table = Table(
      schema: schema,
      id: try Number(tableID.description),
      indexIDs: try indexIDs.map { try Number($0.description) }
    )
    var batch = WriteBatch()
    batch.put(Array(try ValueCodec.encode(.document(Self.catalogDocument(table)))), for: Self.catalogKey(schema.name))
    batch.put(Array(try ValueCodec.encode(.number(try Number(nextID.description)))), for: nextKey)
    try await session.writeBatch(batch)
    return table
  }

  /// Drops a table and every row and secondary-index entry it owns.
  public func drop(_ table: Table) async throws {
    try await ensureCurrent(table)
    var batch = WriteBatch()
    for prefix in [Self.rowPrefix(table.id)] + table.indexIDs.map(Self.indexPrefix) {
      let end = try DocumentCollection.upperBound(after: prefix)
      for item in try await session.scanKeys(from: prefix, to: end) { batch.delete(item.key) }
    }
    batch.delete(Self.catalogKey(table.schema.name))
    try await session.writeBatch(batch)
  }

  /// Inserts a new row, failing if another row has the same primary key.
  public func insert(_ row: Document, into table: Table) async throws {
    try await write(row, into: table, replacing: false)
  }

  /// Inserts or replaces the row with the same primary key.
  public func replace(_ row: Document, in table: Table) async throws {
    try await write(row, into: table, replacing: true)
  }

  /// Sets supplied fields on an existing row and preserves its primary key.
  public func update(_ table: Table, primaryKey: [Value], fields: Document) async throws {
    try await ensureCurrent(table)
    let rowKey = try Self.rowKey(table, primaryKey)
    guard let old = try await readDocument(at: rowKey) else {
      throw KwiftDBError.table("cannot update a missing row")
    }
    var merged = Dictionary(uniqueKeysWithValues: old.fields.map { ($0.name, $0.value) })
    for field in fields { merged[field.name] = field.value }
    let updated = try Document(table.schema.columns.compactMap { column in
      merged[column.name].map { DocumentField(column.name, $0) }
    })
    let normalized = try Self.validatedRow(updated, schema: table.schema)
    let newPrimary = try Self.primaryValues(updated, schema: table.schema)
    guard newPrimary == primaryKey else {
      throw KwiftDBError.table("an update cannot change the primary key")
    }
    try await write(normalized, into: table, replacing: true)
  }

  /// Deletes a row when its primary key exists; missing rows are ignored.
  public func delete(_ table: Table, primaryKey: [Value]) async throws {
    try await ensureCurrent(table)
    let rowKey = try Self.rowKey(table, primaryKey)
    guard let old = try await readDocument(at: rowKey) else { return }
    var batch = WriteBatch()
    batch.delete(rowKey)
    for (index, indexSchema) in table.schema.indexes.enumerated() {
      batch.delete(try Self.indexKey(table.indexIDs[index], indexSchema, old, table.schema))
    }
    try await session.writeBatch(batch)
  }

  /// Executes a validated query and returns rows in deterministic order.
  public func query(_ query: Query) async throws -> [Document] {
    try await ensureCurrent(query.table)
    let chosen = try Self.validate(query)
    let range = try Self.sourceRange(table: query.table, chosen: chosen)
    let candidates = try await readCandidates(query.table, chosen: chosen, range: range)
    let filtered = try candidates.filter { try Self.matches(query.conditions, row: $0, schema: query.table.schema) }
    let ordered = try filtered.sorted { try Self.rowPrecedes($0, $1, query: query) }
    let limited = query.limit.map { Array(ordered.prefix(max(0, $0))) } ?? ordered
    guard !query.columns.isEmpty else { return limited }
    return try limited.map { row in
      try Document(query.table.schema.columns.compactMap { column in
        query.columns.contains(column.name) ? row[column.name].map { DocumentField(column.name, $0) } : nil
      })
    }
  }

  /// Returns a bounded ascending primary-key page. Unlike `query`, this
  /// records the page's key prefix, not the full owner range. Only queries
  /// whose conditions and ordering match their primary key are supported.
  public func queryPage(_ query: Query) async throws -> [Document] {
    try await ensureCurrent(query.table)
    let plan = try Self.validate(query)
    let primary = query.table.schema.primaryKey
    guard let limit = query.limit, limit > 0, plan.indexPosition == nil,
          plan.equalities.count == primary.count - 1,
          plan.range == nil || plan.range?.relation == .greater,
          plan.range == nil || plan.range?.column == primary.last,
          query.orders.map(\.column) == [primary.last!],
          query.orders.allSatisfy({ $0.direction == .ascending }),
          query.conditions.count == plan.equalities.count + (plan.range == nil ? 0 : 1),
          query.columns.isEmpty else {
      throw KwiftDBError.table("a bounded page requires a primary-key prefix and ascending key order")
    }
    let range = try Self.sourceRange(table: query.table, chosen: plan)
    let entries: [KeyValue]
    if let recorded = session as? RecordedRun {
      entries = try await recorded.scanPrefix(from: range.start, to: range.end, count: limit)
    } else {
      entries = Array(try await session.scanKeys(from: range.start, to: range.end).prefix(limit))
    }
    return try entries.map { entry in
      guard case .document(let row) = try ValueCodec.decode(Data(entry.value)) else {
        throw KwiftDBError.corruptStorage("table row is not a document")
      }
      return try Self.validatedRow(row, schema: query.table.schema)
    }
  }

  private func write(_ row: Document, into table: Table, replacing: Bool) async throws {
    try await ensureCurrent(table)
    let normalized = try Self.validatedRow(row, schema: table.schema)
    let primaryValues = try Self.primaryValues(normalized, schema: table.schema)
    let rowKey = try Self.rowKey(table, primaryValues)
    let old = try await readDocument(at: rowKey)
    if old != nil && !replacing { throw KwiftDBError.table("row already exists") }

    var batch = WriteBatch()
    if let old {
      for (index, indexSchema) in table.schema.indexes.enumerated() {
        batch.delete(try Self.indexKey(table.indexIDs[index], indexSchema, old, table.schema))
      }
    }
    for (index, indexSchema) in table.schema.indexes.enumerated() {
      let indexedValues = Self.indexValues(indexSchema, row: normalized)
      let indexKey = try Self.indexKey(table.indexIDs[index], indexSchema, normalized, table.schema)
      if indexSchema.unique, !indexedValues.contains(.null),
         try await hasUniqueConflict(
           indexID: table.indexIDs[index], indexedValues: indexedValues,
           primaryKey: primaryValues, schema: indexSchema
         ) {
        throw KwiftDBError.table("unique index \(indexSchema.name) would contain a duplicate")
      }
      batch.put([], for: indexKey)
    }
    batch.put(Array(try ValueCodec.encode(.document(normalized))), for: rowKey)
    try await session.writeBatch(batch)
  }

  private func hasUniqueConflict(
    indexID: Number,
    indexedValues: [Value],
    primaryKey: [Value],
    schema: IndexSchema
  ) async throws -> Bool {
    let prefix = try KeyEncoder.encodeTuple([.text("i"), .number(indexID)] + indexedValues)
    let end = try DocumentCollection.upperBound(after: prefix)
    for entry in try await session.scanKeys(from: prefix, to: end) {
      let tuple = try KeyDecoder.decodeTuple(entry.key)
      let existingPrimary = Array(tuple.dropFirst(2 + schema.columns.count))
      if existingPrimary != primaryKey { return true }
    }
    return false
  }

  private func readDocument(at key: [UInt8]) async throws -> Document? {
    guard let data = try await session.readKey(key) else { return nil }
    guard case .document(let document) = try ValueCodec.decode(Data(data)) else {
      throw KwiftDBError.corruptStorage("table row is not a document")
    }
    return document
  }

  private func readValue(_ key: [UInt8]) async throws -> Value? {
    guard let bytes = try await session.readKey(key) else { return nil }
    return try ValueCodec.decode(Data(bytes))
  }

  private func ensureCurrent(_ table: Table) async throws {
    guard let value = try await readValue(Self.catalogKey(table.schema.name)),
          case .document(let document) = value,
          document["id"] == .number(table.id) else {
      throw KwiftDBError.table("table is no longer in the catalog: \(table.schema.name)")
    }
  }

  private func readCandidates(_ table: Table, chosen: TablePlan, range: KeyRange) async throws -> [Document] {
    let entries = try await session.scanKeys(from: range.start, to: range.end)
    if let indexPosition = chosen.indexPosition {
      var seen = Set<Data>()
      var rows: [Document] = []
      for entry in entries {
        let tuple = try KeyDecoder.decodeTuple(entry.key)
        let keyStart = 2 + table.schema.indexes[indexPosition].columns.count
        guard tuple.count >= keyStart + table.schema.primaryKey.count else {
          throw KwiftDBError.corruptStorage("short secondary index key")
        }
        let primaryValues = Array(tuple.dropFirst(keyStart))
        let rowKey = try Self.rowKey(table, primaryValues)
        guard seen.insert(Data(rowKey)).inserted else { continue }
        if let row = try await readDocument(at: rowKey) {
          rows.append(try Self.validatedRow(row, schema: table.schema))
        }
      }
      return rows
    }

    return try entries.compactMap { entry in
      guard case .document(let row) = try ValueCodec.decode(Data(entry.value)) else {
        throw KwiftDBError.corruptStorage("table row is not a document")
      }
      return try Self.validatedRow(row, schema: table.schema)
    }
  }

  private static func validate(_ query: Query) throws -> TablePlan {
    try query.table.schema.validate()
    if let limit = query.limit, limit < 0 { throw KwiftDBError.table("query limit cannot be negative") }
    let columns = query.table.schema.columnsByName
    func validateCondition(_ condition: Condition) throws {
      switch condition {
      case .compare(let comparison):
        guard let column = columns[comparison.column] else {
          throw KwiftDBError.table("unknown query column \(comparison.column)")
        }
        guard column.type.isOrderable, comparison.value != .null,
              column.type.accepts(comparison.value) else {
          throw KwiftDBError.table("comparison value does not match orderable column \(comparison.column)")
        }
        _ = try KeyEncoder.encode(comparison.value)
      case .isNull(let name):
        guard let column = columns[name], column.optional else {
          throw KwiftDBError.table("IsNull requires an optional column: \(name)")
        }
      case .allOf(let children), .anyOf(let children):
        for child in children { try validateCondition(child) }
      }
    }
    for condition in query.conditions { try validateCondition(condition) }
    for order in query.orders {
      guard let column = columns[order.column], column.type.isOrderable else {
        throw KwiftDBError.table("Order requires an orderable column: \(order.column)")
      }
    }
    for name in query.columns where columns[name] == nil {
      throw KwiftDBError.table("unknown projected column \(name)")
    }

    let topLevel = query.conditions.compactMap { condition -> Comparison? in
      if case .compare(let comparison) = condition { return comparison }
      return nil
    }
    var candidates = [([String], Int?)]()
    candidates.append((query.table.schema.primaryKey, nil))
    candidates.append(contentsOf: query.table.schema.indexes.indices.map {
      (query.table.schema.indexes[$0].columns, $0)
    })
    var best = TablePlan(indexPosition: nil, equalities: [], range: nil)
    for (indexColumns, indexPosition) in candidates {
      var equalities: [Comparison] = []
      for name in indexColumns {
        guard let equality = topLevel.first(where: { $0.column == name && $0.relation == .equal }) else { break }
        equalities.append(equality)
      }
      let range = equalities.count < indexColumns.count
        ? topLevel.first(where: { $0.column == indexColumns[equalities.count] && $0.relation.isRange })
        : nil
      guard !equalities.isEmpty || range != nil else { continue }
      let improves = equalities.count > best.equalityCount
        || (equalities.count == best.equalityCount && range != nil && best.range == nil)
      if improves { best = TablePlan(indexPosition: indexPosition, equalities: equalities, range: range) }
    }
    return best
  }

  private static func sourceRange(table: Table, chosen: TablePlan) throws -> KeyRange {
    let columns = chosen.indexPosition.map { table.schema.indexes[$0].columns } ?? table.schema.primaryKey
    let id = chosen.indexPosition.map { table.indexIDs[$0] } ?? table.id
    let namespace = chosen.indexPosition == nil ? "t" : "i"
    var parts: [Value] = [.text(namespace), .number(id)]
    for comparison in chosen.equalities.prefix(columns.count) { parts.append(comparison.value) }
    let prefix = try KeyEncoder.encodeTuple(parts)
    guard let range = chosen.range else {
      return KeyRange(start: prefix, end: try DocumentCollection.upperBound(after: prefix))
    }
    let rangePrefix = try KeyEncoder.encodeTuple(parts + [range.value])
    switch range.relation {
    case .greater:
      return KeyRange(start: try DocumentCollection.upperBound(after: rangePrefix), end: try DocumentCollection.upperBound(after: prefix))
    case .greaterOrEqual:
      return KeyRange(start: rangePrefix, end: try DocumentCollection.upperBound(after: prefix))
    case .less:
      return KeyRange(start: prefix, end: rangePrefix)
    case .lessOrEqual:
      return KeyRange(start: prefix, end: try DocumentCollection.upperBound(after: rangePrefix))
    case .equal, .notEqual:
      return KeyRange(start: prefix, end: try DocumentCollection.upperBound(after: prefix))
    }
  }

  private static func matches(_ conditions: [Condition], row: Document, schema: TableSchema) throws -> Bool {
    func matches(_ condition: Condition) throws -> Bool {
      switch condition {
      case .compare(let comparison):
        guard let value = row[comparison.column] else { return false }
        let lhs = try KeyEncoder.encode(value)
        let rhs = try KeyEncoder.encode(comparison.value)
        let less = lhs.lexicographicallyPrecedes(rhs)
        let equal = lhs == rhs
        return switch comparison.relation {
        case .equal: equal
        case .notEqual: !equal
        case .less: less
        case .lessOrEqual: less || equal
        case .greater: !less && !equal
        case .greaterOrEqual: !less
        }
      case .isNull(let name): return row[name] == nil || row[name] == .null
      case .allOf(let conditions):
        for condition in conditions where try !matches(condition) { return false }
        return true
      case .anyOf(let conditions):
        for condition in conditions where try matches(condition) { return true }
        return false
      }
    }
    _ = schema
    for condition in conditions where try !matches(condition) { return false }
    return true
  }

  private static func rowPrecedes(_ lhs: Document, _ rhs: Document, query: Query) throws -> Bool {
    for order in query.orders {
      let left = lhs[order.column] ?? .null
      let right = rhs[order.column] ?? .null
      let leftKey = try KeyEncoder.encode(left)
      let rightKey = try KeyEncoder.encode(right)
      if leftKey != rightKey {
        return order.direction == .ascending
          ? leftKey.lexicographicallyPrecedes(rightKey)
          : rightKey.lexicographicallyPrecedes(leftKey)
      }
    }
    let leftKey = try KeyEncoder.encodeTuple(query.table.schema.primaryKey.compactMap { lhs[$0] })
    let rightKey = try KeyEncoder.encodeTuple(query.table.schema.primaryKey.compactMap { rhs[$0] })
    return leftKey.lexicographicallyPrecedes(rightKey)
  }

  private static func catalogKey(_ name: String) -> [UInt8] {
    (try? KeyEncoder.encodeTuple([.text("c"), .text("table"), .text(name)])) ?? []
  }

  private static func rowPrefix(_ id: Number) -> [UInt8] {
    (try? KeyEncoder.encodeTuple([.text("t"), .number(id)])) ?? []
  }

  private static func indexPrefix(_ id: Number) -> [UInt8] {
    (try? KeyEncoder.encodeTuple([.text("i"), .number(id)])) ?? []
  }

  private static func rowKey(_ table: Table, _ values: [Value]) throws -> [UInt8] {
    guard values.count == table.schema.primaryKey.count else {
      throw KwiftDBError.table("primary key has the wrong number of values")
    }
    for (name, value) in zip(table.schema.primaryKey, values) {
      guard value != .null, let column = table.schema.columnsByName[name], column.type.accepts(value) else {
        throw KwiftDBError.table("primary key value does not match column \(name)")
      }
    }
    let keyValues: [Value] = [.text("t"), .number(table.id)] + values
    return try KeyEncoder.encodeTuple(keyValues)
  }

  private static func primaryValues(_ row: Document, schema: TableSchema) throws -> [Value] {
    try schema.primaryKey.map { name in
      guard let value = row[name] else { throw KwiftDBError.invalidRow("missing primary key field \(name)") }
      return value
    }
  }

  private static func indexKey(_ id: Number, _ index: IndexSchema, _ row: Document, _ schema: TableSchema) throws -> [UInt8] {
    let indexed = index.columns.map { name -> Value in row[name] ?? .null }
    let primary = try primaryValues(row, schema: schema)
    return try KeyEncoder.encodeTuple([.text("i"), .number(id)] + indexed + primary)
  }

  private static func validatedRow(_ row: Document, schema: TableSchema) throws -> Document {
    var normalized: [DocumentField] = []
    for field in row {
      guard let column = schema.columnsByName[field.name] else {
        throw KwiftDBError.invalidRow("unknown field \(field.name)")
      }
      if field.value == .null {
        guard column.optional else { throw KwiftDBError.invalidRow("required column \(field.name) is Null") }
      } else if !column.type.accepts(field.value) {
        throw KwiftDBError.invalidRow("field \(field.name) has the wrong value type")
      }
    }
    for column in schema.columns {
      guard let value = row[column.name] else {
        if !column.optional { throw KwiftDBError.invalidRow("missing required field \(column.name)") }
        continue
      }
      if value == .null { continue }
      normalized.append(DocumentField(column.name, value))
    }
    return try Document(normalized)
  }

  private static func catalogDocument(_ table: Table) throws -> Document {
    let columns = try table.schema.columns.map { column in
      try Document([
        DocumentField("name", .text(column.name)),
        DocumentField("type", .text(column.type.rawValue)),
        DocumentField("optional", .bool(column.optional)),
      ])
    }
    let indexes = try zip(table.schema.indexes, table.indexIDs).map { index, id in
      try Document([
        DocumentField("name", .text(index.name)),
        DocumentField("id", .number(id)),
        DocumentField("columns", .list(index.columns.map(Value.text))),
        DocumentField("unique", .bool(index.unique)),
      ])
    }
    return try Document([
      DocumentField("id", .number(table.id)),
      DocumentField("columns", .list(columns.map(Value.document))),
      DocumentField("primaryKey", .list(table.schema.primaryKey.map(Value.text))),
      DocumentField("indexes", .list(indexes.map(Value.document))),
    ])
  }

  private static func table(from value: Value, name: String) throws -> Table {
    guard case .document(let record) = value,
          case .number(let id)? = record["id"],
          case .list(let rawColumns)? = record["columns"],
          case .list(let rawPrimary)? = record["primaryKey"],
          case .list(let rawIndexes)? = record["indexes"] else {
      throw KwiftDBError.corruptStorage("malformed table catalog record")
    }
    var columns: [Column] = []
    for raw in rawColumns {
      guard case .document(let item) = raw,
            case .text(let name)? = item["name"],
            case .text(let typeName)? = item["type"], let type = ColumnType(rawValue: typeName),
            case .bool(let optional)? = item["optional"] else {
        throw KwiftDBError.corruptStorage("malformed table column record")
      }
      columns.append(Column(name: name, type: type, optional: optional))
    }
    let primary = try rawPrimary.map { value -> String in
      guard case .text(let name) = value else { throw KwiftDBError.corruptStorage("malformed primary key") }
      return name
    }
    var indexes: [IndexSchema] = []
    var ids: [Number] = []
    for raw in rawIndexes {
      guard case .document(let item) = raw,
            case .text(let name)? = item["name"],
            case .number(let indexID)? = item["id"],
            case .list(let rawNames)? = item["columns"],
            case .bool(let unique)? = item["unique"] else {
        throw KwiftDBError.corruptStorage("malformed index record")
      }
      let names = try rawNames.map { value -> String in
        guard case .text(let name) = value else { throw KwiftDBError.corruptStorage("malformed index column") }
        return name
      }
      indexes.append(IndexSchema(name: name, columns: names, unique: unique))
      ids.append(indexID)
    }
    let schema = try TableSchema(name: name, columns: columns, primaryKey: primary, indexes: indexes)
    return Table(schema: schema, id: id, indexIDs: ids)
  }

  private static func indexValues(_ index: IndexSchema, row: Document) -> [Value] {
    index.columns.map { row[$0] ?? .null }
  }

  private static func unsignedInteger(_ value: Value) throws -> DatabaseUInt {
    guard case .number(let number) = value, let text = number.unsignedIntegerText else {
      throw KwiftDBError.corruptStorage("expected a non-negative integer id")
    }
    return try DatabaseUInt(text)
  }
}

private struct TablePlan {
  let indexPosition: Int?
  let equalities: [Comparison]
  let range: Comparison?
  var equalityCount: Int { equalities.count }
}

private extension Relation {
  var isRange: Bool {
    switch self {
    case .less, .lessOrEqual, .greater, .greaterOrEqual: true
    case .equal, .notEqual: false
    }
  }
}
