import Foundation

/// The authenticated identity and durable client id passed to sync functions.
public struct Caller: Sendable, Equatable {
  /// The server-assigned client number, or zero before first welcome.
  public var client: Number
  /// The value returned by the server's authentication function.
  public var identity: Value

  /// Creates caller context for a query or mutation function.
  public init(client: Number, identity: Value) {
    self.client = client
    self.identity = identity
  }
}

/// A typed failure raised by a query or mutation function.
public enum FunctionFailure: Error, Sendable, Equatable, LocalizedError {
  /// The underlying document database operation failed.
  case database(String)
  /// The table schema or row operation failed.
  case table(String)
  /// The function refused this caller or argument set.
  case refused(String)
  /// No function with the requested name was registered.
  case noSuchFunction(String)

  /// A concise text representation sent to sync clients.
  public var errorDescription: String? {
    switch self {
    case .database(let reason): "Database: \(reason)"
    case .table(let reason): "Table: \(reason)"
    case .refused(let reason): reason
    case .noSuchFunction(let name): "NoSuchFunction: \(name)"
    }
  }
}

/// A deterministic, read-only application query available to sync clients.
public struct QueryFunction: Sendable {
  /// Unique query function name.
  public let name: String
  /// Runs only through the supplied recorded run and caller context.
  public let run: @Sendable (RecordedRun, Caller, Value) async throws -> Value

  /// Registers a query function.
  public init(
    name: String,
    run: @escaping @Sendable (RecordedRun, Caller, Value) async throws -> Value
  ) {
    self.name = name
    self.run = run
  }
}

/// A deterministic application mutation.
///
/// By default sync clients may call it. A mutation with `.server` access is for
/// server code only, through `SyncServer.mutate`.
public struct MutationFunction: Sendable {
  /// Who may call a mutation.
  public enum Access: Sendable, Equatable {
    /// Sync clients and server code.
    case client
    /// Server code only. A client frame that names the function fails as
    /// `FunctionFailure.noSuchFunction`, exactly as if it were not registered.
    case server
  }

  /// Unique mutation function name.
  public let name: String
  /// Who may call the mutation.
  public let access: Access
  /// Whether a replica applies the mutation to its local view before the
  /// server answers. A mutation whose effect no client reads, such as one
  /// that only hands data to the server, sets this false: replicas queue and
  /// send it but never run it, so it costs them no query refresh.
  public let speculative: Bool
  /// Runs through the supplied recorded run and caller context.
  public let run: @Sendable (RecordedRun, Caller, Value) async throws -> Value

  /// Registers a mutation function.
  public init(
    name: String,
    access: Access = .client,
    speculative: Bool = true,
    run: @escaping @Sendable (RecordedRun, Caller, Value) async throws -> Value
  ) {
    self.name = name
    self.access = access
    self.speculative = speculative
    self.run = run
  }
}

/// The query and mutation registry shared by a sync server and replicas.
public struct Functions: Sendable {
  /// Queries callable by name.
  public let queries: [QueryFunction]
  /// Mutations callable by name.
  public let mutations: [MutationFunction]

  /// Validates and stores the application's function definitions.
  public init(queries: [QueryFunction], mutations: [MutationFunction]) throws {
    guard Set(queries.map(\.name)).count == queries.count else {
      throw KwiftDBError.invalidSchema("query function names must be unique")
    }
    guard Set(mutations.map(\.name)).count == mutations.count else {
      throw KwiftDBError.invalidSchema("mutation function names must be unique")
    }
    guard queries.allSatisfy({ !$0.name.isEmpty }), mutations.allSatisfy({ !$0.name.isEmpty }) else {
      throw KwiftDBError.invalidSchema("function names cannot be empty")
    }
    self.queries = queries
    self.mutations = mutations
  }

  internal func query(named name: String) -> QueryFunction? { queries.first { $0.name == name } }
  internal func mutation(named name: String) -> MutationFunction? { mutations.first { $0.name == name } }
}
