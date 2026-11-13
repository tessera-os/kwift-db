/// A table a maintenance job wrote, ready to join the manifest.
struct WrittenTable: Sendable {
  var record: TableRecord
  var size: UInt64
}
