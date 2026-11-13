/// A mutable tree node that collects what an encoder writes.
///
/// `Encodable` types write into containers that may be created, nested and
/// finished in any order (and `superEncoder()` hands out encoders that are
/// filled after the fact), so the encoder first builds this tree of reference
/// nodes and converts it to an immutable `Value` once encoding has finished.
/// Keyed nodes remember insertion order, which becomes the document's field
/// order.
final class EncodingNode {
  /// What the node has become.
  enum Kind { case empty, leaf, keyed, unkeyed }

  /// The current shape. A node starts `.empty` and commits to one shape.
  private(set) var kind = Kind.empty
  private var leafValue = Value.null
  private var names: [String] = []
  private var children: [EncodingNode] = []
  private var positions: [String: Int] = [:]

  /// The number of children of a keyed or unkeyed node.
  var count: Int { children.count }

  /// Makes this node a single value. Encoding a second value replaces the first.
  func setLeaf(_ value: Value) {
    precondition(kind == .empty || kind == .leaf, "Attempt to encode a value into a container")
    kind = .leaf
    leafValue = value
  }

  /// Makes this node keyed (a document). Calling it again keeps existing fields.
  func becomeKeyed() {
    precondition(kind == .empty || kind == .keyed, "Attempt to create a keyed container here")
    kind = .keyed
  }

  /// Makes this node unkeyed (a list). Calling it again keeps existing elements.
  func becomeUnkeyed() {
    precondition(kind == .empty || kind == .unkeyed, "Attempt to create an unkeyed container here")
    kind = .unkeyed
  }

  /// Returns a fresh child for the field `name`.
  ///
  /// Encoding a key twice replaces the earlier value but keeps its position.
  func child(forKey name: String) -> EncodingNode {
    becomeKeyed()
    let node = EncodingNode()
    if let index = positions[name] {
      children[index] = node
    } else {
      positions[name] = children.count
      names.append(name)
      children.append(node)
    }
    return node
  }

  /// Takes over everything `other` has collected.
  ///
  /// Containers encode a value into a detached node first, so a failure leaves
  /// no half-written field behind, and then adopt the finished node.
  func adopt(_ other: EncodingNode) {
    kind = other.kind
    leafValue = other.leafValue
    names = other.names
    children = other.children
    positions = other.positions
  }

  /// Appends and returns a fresh child at the end of the list.
  func appendChild() -> EncodingNode {
    becomeUnkeyed()
    let node = EncodingNode()
    children.append(node)
    return node
  }

  /// Converts the tree to a `Value`.
  ///
  /// A node nothing was written to (a type that encodes no properties)
  /// becomes an empty document, like `JSONEncoder`'s `{}`.
  func resolve() throws -> Value {
    switch kind {
    case .empty:
      return .document(try Document())
    case .leaf:
      return leafValue
    case .keyed:
      var fields: [DocumentField] = []
      fields.reserveCapacity(children.count)
      for (name, child) in zip(names, children) {
        fields.append(DocumentField(name, try child.resolve()))
      }
      return .document(try Document(fields: fields))
    case .unkeyed:
      return .list(try children.map { try $0.resolve() })
    }
  }
}
