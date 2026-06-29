require_relative "lib/filedbv2/version"

Gem::Specification.new do |spec|
  spec.name          = "filedbv2"
  spec.version       = FileDBv2::VERSION
  spec.authors       = ["srjn45"]
  spec.email         = ["29410402+srjn45@users.noreply.github.com"]
  spec.summary       = "Ruby gRPC client for FileDB v2"
  spec.description   = "A thin, idiomatic Ruby wrapper over the FileDB v2 gRPC API."
  spec.homepage      = "https://github.com/srjn45/filedbv2"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.1"

  spec.files = Dir["lib/**/*.rb"] + ["README.md", "filedbv2.gemspec"]
  spec.require_paths = ["lib"]

  spec.add_dependency "grpc",             "~> 1.60"
  spec.add_dependency "google-protobuf",  "~> 3.25"

  spec.metadata["homepage_uri"]    = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/srjn45/filedbv2/tree/main/clients/ruby"
end
