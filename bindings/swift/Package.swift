// swift-tools-version:5.9
//
// FireMQ for Swift: UniFFI bindings over the Rust client.
//
// The package links the Rust library, libfiremq_client_ffi, from the linker search path:
//
//   cargo build -p firemq-client-ffi --release
//   swift build -Xlinker -L../../target/release
//   FIREMQ_URL=fire://guest:guest@localhost:5673 \
//     DYLD_LIBRARY_PATH=../../target/release swift run -Xlinker -L../../target/release hello-firemq
//
// For app distribution, build-xcframework.sh packages the library as an XCFramework (needs
// Xcode); replace the FireMQFFI system library with a binaryTarget pointing at it.

import PackageDescription

let package = Package(
    name: "FireMQ",
    platforms: [.macOS(.v12), .iOS(.v15)],
    products: [
        .library(name: "FireMQ", targets: ["FireMQ"]),
        .executable(name: "hello-firemq", targets: ["HelloFireMQ"]),
        .executable(name: "firemq-conformance-driver", targets: ["ConformanceDriver"]),
    ],
    targets: [
        .systemLibrary(name: "FireMQFFI", path: "Sources/FireMQFFI"),
        .target(name: "FireMQ", dependencies: ["FireMQFFI"], path: "Sources/FireMQ"),
        .executableTarget(
            name: "HelloFireMQ", dependencies: ["FireMQ"], path: "Sources/HelloFireMQ"),
        .executableTarget(
            name: "ConformanceDriver", dependencies: ["FireMQ"], path: "Sources/ConformanceDriver"),
    ]
)
