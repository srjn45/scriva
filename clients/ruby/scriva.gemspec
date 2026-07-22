require_relative "lib/scriva/version"

Gem::Specification.new do |spec|
  spec.name          = "scriva"
  spec.version       = Scriva::VERSION
  spec.authors       = ["srjn45"]
  spec.email         = ["29410402+srjn45@users.noreply.github.com"]
  spec.summary       = "Ruby gRPC client for ScrivaDB"
  spec.description   = "A thin, idiomatic Ruby wrapper over the ScrivaDB gRPC API."
  spec.homepage      = "https://github.com/srjn45/scriva"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.1"

  spec.files = Dir["lib/**/*.rb"] + ["README.md", "scriva.gemspec"]
  spec.require_paths = ["lib"]

  spec.add_dependency "grpc",             "~> 1.60"
  spec.add_dependency "google-protobuf",  "~> 3.25"

  spec.metadata["homepage_uri"]    = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/srjn45/scriva/tree/main/clients/ruby"
end
