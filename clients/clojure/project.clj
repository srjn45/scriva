(defproject io.github.srjn45/scriva-client-clojure "1.2.1"
  :description "Official Clojure gRPC client for ScrivaDB — idiomatic maps and lazy seqs over the Java gRPC stubs."
  :url "https://github.com/srjn45/scriva"
  :license {:name "MIT License"
            :url "https://github.com/srjn45/scriva/blob/main/clients/clojure/LICENSE"}

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.google.protobuf/protobuf-java "3.25.3"]
                 [io.grpc/grpc-netty-shaded "1.64.0"]
                 ;; Exclude proto-google-common-protos: we generate our own
                 ;; com.google.api.* classes from the vendored google/api protos,
                 ;; so pulling the jar in too would duplicate them.
                 [io.grpc/grpc-protobuf "1.64.0"
                  :exclusions [com.google.api.grpc/proto-google-common-protos]]
                 [io.grpc/grpc-stub "1.64.0"]
                 [javax.annotation/javax.annotation-api "1.3.2"]]

  ;; ── gRPC stub generation ────────────────────────────────────────────────
  ;; lein-protoc downloads protoc + the grpc-java plugin automatically and
  ;; compiles the proto into Java stubs, which javac then builds. The stubs are
  ;; generated from the single source of truth (../../proto/scriva.proto); the
  ;; local proto/ dir vendors the google/api annotations protoc does not bundle.
  :plugins [[lein-protoc "0.5.0"]]
  :protoc-version "3.25.3"
  :protoc-grpc {:version "1.64.0"}
  :proto-source-paths ["proto" "../../proto"]
  :proto-target-path "target/generated-sources/protobuf"
  :java-source-paths ["target/generated-sources/protobuf"]
  :prep-tasks [["protoc"] "javac" "compile"]

  :profiles {:dev {:dependencies [[io.grpc/grpc-inprocess "1.64.0"]]}}

  ;; ── Clojars publishing (the Clojure-standard registry) ──────────────────
  ;; `lein deploy clojars` reads CLOJARS_USERNAME / CLOJARS_TOKEN from the env
  ;; (see publish-clients.yml). Nothing is published from here.
  :deploy-repositories [["clojars" {:url "https://repo.clojars.org"
                                    :username :env/clojars_username
                                    :password :env/clojars_token
                                    :sign-releases false}]]

  :scm {:name "git" :url "https://github.com/srjn45/scriva"}
  :pom-addition [:developers [:developer
                              [:id "srjn45"]
                              [:name "srjn45"]
                              [:url "https://github.com/srjn45"]]]

  :repl-options {:init-ns scriva.client})
