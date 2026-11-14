# KwiftDB

KwiftDB is a Swift 6 database with replicas that stay in sync with a server.
It is the Swift implementation of KiraDB: one ordered key-value store with a
document face (collections of documents by key) and a table face (typed
tables, rows and indexes), plus the sync protocol that keeps each client's
replica in step with the database of record. KiraDB's specification
(`docs/SPEC.md` in [kira-lang-com/kira-db](https://github.com/kira-lang-com/kira-db))
is the rule for both.

| Product | What it holds |
| --- | --- |
| `KwiftDB` | The store (`Store`, `Database`), documents and tables (`TableCatalog`), durable streams (`DurableStreams`), registered functions, the sync server (`SyncServer`) and replicas (`Replica`), and replication across servers (`ReplicationNode`, `ReplicationCoordinator`). No networking, so it also builds for WebAssembly. |
| `KwiftDBQUIC` | Sync and replication over QUIC bidirectional streams, on SwiftNIO QUIC. Its types need the 27 releases on Apple platforms. |

KwiftDB runs on macOS 13, iOS 16 and Linux; `KwiftDBQUIC` needs macOS and
iOS 27 on Apple platforms. The storage engine is compiled with optimization
in debug builds too, because apps under development read their replica on
every screen.

## Add KwiftDB to a package

```swift
dependencies: [
  .package(url: "https://github.com/tessera-os/kwift-db.git", from: "26.10.4"),
],
targets: [
  .target(name: "MyApp", dependencies: [
    .product(name: "KwiftDB", package: "kwift-db"),
    .product(name: "KwiftDBQUIC", package: "kwift-db"),
  ]),
]
```

## Develop

```sh
swift build
swift test --filter KwiftDBTests
```

CI runs every test on macOS 27, the iOS 27 simulator and Linux. KwiftDB is
developed in the Tessera monorepo and mirrored here by Cairn; the version is
the `version` constant in `Package.swift`, and `RELEASE.md` holds the current
week's notes.
