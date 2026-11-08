#!/usr/bin/env bash
# Packages the Rust library as FireMQFFI.xcframework for macOS (arm64 + x86_64), iOS and the
# iOS simulator, so apps can depend on the package without a Rust toolchain. Also writes
# FireMQFFI.xcframework.zip and its SwiftPM checksum, which the release attaches.
#
# Needs Xcode: `xcodebuild -create-xcframework` is not part of the Command Line Tools.
# CI runs it on macos-latest. To use the result, replace the FireMQFFI systemLibrary target
# in Package.swift with:
#
#   .binaryTarget(name: "FireMQFFI", path: "build/FireMQFFI.xcframework")
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
target="${CARGO_TARGET_DIR:-$root/target}"
out="$here/build"

targets=(aarch64-apple-darwin x86_64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios)
rustup target add "${targets[@]}"
for t in "${targets[@]}"; do
  cargo build --manifest-path "$root/Cargo.toml" -p firemq-client-ffi --release --target "$t"
done

rm -rf "$out"
mkdir -p "$out/headers" "$out/macos" "$out/ios-simulator"
cp "$here/Sources/FireMQFFI/FireMQFFI.h" "$out/headers/"
# The static library is inside the framework, so the module map does not link anything.
cat > "$out/headers/module.modulemap" <<'EOF'
module FireMQFFI {
    header "FireMQFFI.h"
    export *
}
EOF

lib=libfiremq_client_ffi.a
lipo -create "$target/aarch64-apple-darwin/release/$lib" "$target/x86_64-apple-darwin/release/$lib" \
  -output "$out/macos/$lib"
lipo -create "$target/aarch64-apple-ios-sim/release/$lib" "$target/x86_64-apple-ios/release/$lib" \
  -output "$out/ios-simulator/$lib"

xcodebuild -create-xcframework \
  -library "$out/macos/$lib" -headers "$out/headers" \
  -library "$target/aarch64-apple-ios/release/$lib" -headers "$out/headers" \
  -library "$out/ios-simulator/$lib" -headers "$out/headers" \
  -output "$out/FireMQFFI.xcframework"

cd "$out"
rm -f FireMQFFI.xcframework.zip
ditto -c -k --sequesterRsrc --keepParent FireMQFFI.xcframework FireMQFFI.xcframework.zip
# SwiftPM's checksum for a binaryTarget is the SHA-256 of the zip.
shasum -a 256 FireMQFFI.xcframework.zip > FireMQFFI.xcframework.zip.sha256

echo "built $out/FireMQFFI.xcframework"
echo "built $out/FireMQFFI.xcframework.zip, checksum $(cut -d' ' -f1 FireMQFFI.xcframework.zip.sha256)"
