# Releasing FireMQ

A release is one version tag, `vX.Y.Z`, on `main`. Two workflows run when it is pushed:

- [`release.yml`](.github/workflows/release.yml) builds the broker for Linux, macOS and
  Windows, the C library archives (`firemq-ffi-<target>`) and the Swift XCFramework, creates
  the GitHub release with all of them, and pushes the container image
  `ghcr.io/firemq/firemq:X.Y.Z` and `:latest`.
- [`publish.yml`](.github/workflows/publish.yml) publishes the crates to crates.io, the Python
  wheels to PyPI and the Ruby gems to RubyGems. It tests the Linux wheel and gem against a
  broker first.

The SDKs in their own repositories (Go, TypeScript, Java and Kotlin, .NET, Elixir, PHP and
Swift) are released by tagging those repositories.

## Cut a release

1. Set the new version everywhere. The publish workflow fails if these differ from each other
   or from the tag:
   - `Cargo.toml`: `[workspace.package] version` and the `version` of each `firemq-*` entry
     in `[workspace.dependencies]`
   - `bindings/python/pyproject.toml`
   - `bindings/ruby/firemq.gemspec` and `VERSION` in `bindings/ruby/lib/firemq.rb`
   - `bindings/dart/pubspec.yaml`, with a `bindings/dart/CHANGELOG.md` entry
2. Add the release to `CHANGELOG.md` and merge.
3. Tag `main` and push the tag:

   ```bash
   git tag -a vX.Y.Z -m "FireMQ X.Y.Z"
   git push origin vX.Y.Z
   ```

4. When both workflows have finished:
   - **Homebrew.** The tap's *Update formula* workflow picks up the release daily. To update
     it at once, run that workflow in [FireMQ/homebrew-tap](https://github.com/FireMQ/homebrew-tap).
   - **Swift.** In [FireMQ/firemq-swift](https://github.com/FireMQ/firemq-swift), point
     `Package.swift` at the new `FireMQFFI.xcframework.zip` with the checksum from its
     `.sha256` file, copy `bindings/swift/Sources/FireMQ`, and tag the same version.
   - **Dart.** From `bindings/dart`, run `dart pub publish`.

To publish a tag again, for example after setting up a registry, run *Publish* from the
Actions tab with the tag. Versions already on a registry are skipped.

## Registry setup

Each registry is skipped until it is set up. Setup is done once, by an owner of the package
on the registry:

| Registry | Setup |
|---|---|
| crates.io | Create an API token with the `publish-new` and `publish-update` scopes and store it as the repository secret `CARGO_REGISTRY_TOKEN`. |
| PyPI | Add a trusted publisher (a pending one before the first release) for project `firemq`: owner `FireMQ`, repository `firemq`, workflow `publish.yml`, environment `pypi`. Then set the repository variable `PYPI_TRUSTED_PUBLISHER` to `true`. |
| RubyGems | Add a trusted publisher (a pending one before the first release) for gem `firemq` with the same repository, workflow `publish.yml` and environment `rubygems`. Then set the repository variable `RUBYGEMS_TRUSTED_PUBLISHER` to `true`. A pending publisher also needs the repository variable `RUBYGEMS_OWNER` set to the numeric account ID that its page shows, because RubyGems rejects the first push of a new gem that doesn't name its owner. |
| pub.dev | The first version is published by hand with `dart pub publish`. |
| GitHub Container Registry | Nothing; the workflow uses `GITHUB_TOKEN`. |

New crates on crates.io are rate-limited, so the first publish of all eight crates takes about
half an hour: the job waits and retries.
