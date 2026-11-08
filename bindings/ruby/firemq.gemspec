# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name = "firemq"
  s.version = "0.1.0"
  s.summary = "Ruby client for FireMQ, built on the Rust client"
  s.description = "UniFFI bindings over firemq-client, the Rust client for the FireMQ message broker."
  s.authors = ["The FireMQ Authors"]
  s.homepage = "https://github.com/FireMQ/firemq"
  s.license = "Apache-2.0"
  s.required_ruby_version = ">= 3.1"
  # Native libraries copied next to the generated bindings are packaged too.
  s.files = Dir["lib/**/*.rb"] + Dir["lib/firemq/*.{so,dylib,dll}"]
  s.require_paths = ["lib"]
  s.add_dependency "ffi", "~> 1.16"
end
