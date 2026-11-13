/// A sync mutation a follower asks the leader to run.
///
/// A follower cannot run a mutation itself: the leader runs every mutation, one
/// at a time, against the newest data (spec section 9.6). It forwards the call
/// instead, as the notes of a `forward` message with an empty batch.
struct MutationRequest: Sendable, Equatable {
  /// The durable id of the calling client.
  var client: Number
  /// The caller's identity, as its `authenticate` function answered it.
  var identity: Value
  /// The client's number for this mutation.
  var number: Number
  /// The mutation function's name.
  var name: String
  /// The mutation function's arguments.
  var arguments: Value
}

/// A request a follower forwards to the leader as a note.
enum ReplicatedRequest: Sendable, Equatable {
  /// Run a client's mutation.
  case mutation(MutationRequest)
  /// Take the next client id (spec section 8.4: a `hello` with client 0).
  case allocation
  /// Run a mutation for server code, as `identity`, with no client numbering.
  case serverMutation(name: String, arguments: Value, identity: Value)

  /// The request a forward's notes carry, or nil when they carry none (the
  /// forward is a ready batch, as `SyncServer.change` forwards).
  init?(notes: [Value]) throws {
    guard case .document(let document)? = notes.first, case .text(let kind)? = document["request"] else { return nil }
    switch kind {
    case "mutation":
      guard case .number(let client)? = document["client"], let identity = document["identity"],
        case .number(let number)? = document["number"], case .text(let name)? = document["name"],
        let arguments = document["arguments"]
      else { throw KwiftDBError.invalidEncoding("malformed mutation request") }
      self = .mutation(MutationRequest(client: client, identity: identity, number: number, name: name, arguments: arguments))
    case "allocation": self = .allocation
    case "server-mutation":
      guard case .text(let name)? = document["name"], let arguments = document["arguments"], let identity = document["identity"]
      else { throw KwiftDBError.invalidEncoding("malformed server mutation request") }
      self = .serverMutation(name: name, arguments: arguments, identity: identity)
    default: throw KwiftDBError.invalidEncoding("unknown replicated request \(kind)")
    }
  }

  /// The notes that carry this request.
  var notes: [Value] {
    get throws {
      switch self {
      case .mutation(let request):
        [.document(try Document([
          DocumentField("request", .text("mutation")), DocumentField("client", .number(request.client)),
          DocumentField("identity", request.identity), DocumentField("number", .number(request.number)),
          DocumentField("name", .text(request.name)), DocumentField("arguments", request.arguments),
        ]))]
      case .allocation:
        [.document(try Document([DocumentField("request", .text("allocation"))]))]
      case .serverMutation(let name, let arguments, let identity):
        [.document(try Document([
          DocumentField("request", .text("server-mutation")), DocumentField("name", .text(name)),
          DocumentField("arguments", arguments), DocumentField("identity", identity),
        ]))]
      }
    }
  }
}

/// What a replicated sync entry's notes tell every member about the entry.
///
/// The leader writes one with each entry it creates for a request. Every
/// member reads it as it applies the entry, so a client's result reaches the
/// session that holds the client wherever the mutation ran.
enum ReplicatedNote: Sendable, Equatable {
  /// A client's mutation was answered; its applied number is now `applied`.
  case mutation(client: Number, number: Number, applied: Number, outcome: SyncMutationOutcome)
  /// A client id was taken.
  case allocation(client: Number)
  /// A server-side mutation was answered.
  case serverMutation(outcome: SyncMutationOutcome)

  /// The first note of `notes` that is a sync note, or nil for entries
  /// written by other means, such as `SyncServer.change` or an election.
  init?(notes: [Value]) throws {
    guard case .document(let document)? = notes.first, case .text(let kind)? = document["note"] else { return nil }
    switch kind {
    case "mutation":
      guard case .number(let client)? = document["client"], case .number(let number)? = document["number"],
        case .number(let applied)? = document["applied"], case .document(let outcome)? = document["outcome"]
      else { throw KwiftDBError.invalidEncoding("malformed mutation note") }
      self = .mutation(client: client, number: number, applied: applied, outcome: Self.outcome(outcome))
    case "server-mutation":
      guard case .document(let outcome)? = document["outcome"] else { throw KwiftDBError.invalidEncoding("malformed server mutation note") }
      self = .serverMutation(outcome: Self.outcome(outcome))
    case "allocation":
      guard case .number(let client)? = document["client"] else { throw KwiftDBError.invalidEncoding("malformed allocation note") }
      self = .allocation(client: client)
    default: throw KwiftDBError.invalidEncoding("unknown replicated note \(kind)")
    }
  }

  /// The notes that carry this record.
  var notes: [Value] {
    get throws {
      switch self {
      case .mutation(let client, let number, let applied, let outcome):
        return [.document(try Document([
          DocumentField("note", .text("mutation")), DocumentField("client", .number(client)),
          DocumentField("number", .number(number)), DocumentField("applied", .number(applied)),
          DocumentField("outcome", .document(try Self.document(outcome))),
        ]))]
      case .serverMutation(let outcome):
        return [.document(try Document([
          DocumentField("note", .text("server-mutation")), DocumentField("outcome", .document(try Self.document(outcome))),
        ]))]
      case .allocation(let client):
        return [.document(try Document([DocumentField("note", .text("allocation")), DocumentField("client", .number(client))]))]
      }
    }
  }

  private static func outcome(_ document: Document) -> SyncMutationOutcome {
    if let value = document["value"] { return .value(value) }
    if case .text(let reason)? = document["failure"] { return .failure(reason) }
    return .skipped
  }

  private static func document(_ outcome: SyncMutationOutcome) throws -> Document {
    let field: DocumentField =
      switch outcome {
      case .value(let value): DocumentField("value", value)
      case .failure(let reason): DocumentField("failure", .text(reason))
      case .skipped: DocumentField("skipped", .bool(true))
      }
    return try Document([field])
  }
}
