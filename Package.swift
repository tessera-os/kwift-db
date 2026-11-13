// swift-tools-version: 6.4
import PackageDescription

/// Settings SwiftNIO QUIC's public API needs from code that uses it.
/// Swift 6.4 language settings every target uses: approachable concurrency
/// (nonisolated async functions run on the caller's actor; conformances may be
/// isolated), explicit `any`, and imports that must name what they use.
let languageSettings: [SwiftSetting] = [
  .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
  .enableUpcomingFeature("InferIsolatedConformances"),
  .enableUpcomingFeature("ExistentialAny"),
  .enableUpcomingFeature("MemberImportVisibility"),
]

/// SwiftNIO QUIC gates its API on `anyAppleOS` availability.
let quicSettings: [SwiftSetting] = languageSettings + [
  .enableExperimentalFeature("AnyAppleOSAvailability")
]

let package = Package(
  name: "KwiftDB",
  // The database runs on older systems. SwiftNIO QUIC needs the 27 releases on
  // Apple platforms, so every KwiftDBQUIC type is `@available(anyAppleOS 27, *)`.
  platforms: [.iOS(.v16), .macOS(.v13)],
  products: [
    .library(name: "KwiftDB", targets: ["KwiftDB"]),
    .library(name: "KwiftDBQUIC", targets: ["KwiftDBQUIC"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0"),
    .package(url: "https://github.com/apple/swift-nio-quic.git", .upToNextMinor(from: "0.6.0")),
    .package(url: "https://github.com/apple/swift-nio-quic-helpers.git", .upToNextMinor(from: "0.1.1")),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.12.1"),
  ],
  targets: [
    // The database, its functions and the sync engines. No networking, so it
    // also builds for WebAssembly.
    .target(
      name: "KwiftDB",
      dependencies: [.product(name: "Logging", package: "swift-log")],
      // The storage engine is optimized in debug builds too: unoptimized, its
      // generic byte comparisons and codecs run about 16 times slower, and
      // apps under development read their replica on every screen.
      swiftSettings: languageSettings + [.unsafeFlags(["-O"], .when(configuration: .debug))]
    ),
    // The sync engines over QUIC (spec section 8.6), on SwiftNIO.
    .target(
      name: "KwiftDBQUIC",
      dependencies: [
        "KwiftDB",
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio"),
        .product(name: "NIOQUIC", package: "swift-nio-quic"),
        .product(name: "NIOQUICHelpers", package: "swift-nio-quic-helpers"),
        .product(name: "Logging", package: "swift-log"),
      ],
      swiftSettings: quicSettings
    ),
    .testTarget(name: "KwiftDBTests", dependencies: ["KwiftDB"], swiftSettings: languageSettings),
    .testTarget(
      name: "KwiftDBQUICTests",
      dependencies: ["KwiftDB", "KwiftDBQUIC"],
      resources: [.copy("Fixtures")],
      swiftSettings: quicSettings
    ),
  ],
  swiftLanguageModes: [.v6]
)
