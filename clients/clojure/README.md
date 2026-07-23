# ScrivaDB — Clojure Client

Idiomatic Clojure gRPC client for [ScrivaDB](../../README.md). Functions take and
return plain Clojure maps; the streaming `find`, `watch`, `aggregate` and
`snapshot` RPCs return lazy seqs. It wraps the generated Java gRPC stubs.

**Clojars coordinates:** `io.github.srjn45/scriva-client-clojure {:mvn/version "1.2.1"}`

---

## Requirements

- JDK 11+
- [Leiningen](https://leiningen.org/) 2.9+
- A running ScrivaDB server (`make run` from the repo root) for the example / live use

The Java gRPC stubs are generated from `../../proto/scriva.proto` at build time
by [`lein-protoc`](https://github.com/LiaisonTechnologies/lein-protoc), which
downloads `protoc` and the grpc-java plugin automatically. The `google/api`
annotations `protoc` does not bundle are vendored under `proto/`.

---

## Build

```bash
cd clients/clojure
lein javac        # generate + compile the Java stubs
lein test         # run the hermetic in-process tests
lein install      # build + install the jar into ~/.m2
```

---

## Install

### Leiningen (`project.clj`)

```clojure
[io.github.srjn45/scriva-client-clojure "1.2.1"]
```

### deps.edn

```clojure
io.github.srjn45/scriva-client-clojure {:mvn/version "1.2.1"}
```

---

## Quick start

```clojure
(require '[scriva.client :as scriva])

(let [db (scriva/connect {:host "localhost" :port 5433 :api-key "dev-key"})]
  (try
    (scriva/create-collection db "users")

    (let [id (scriva/insert db "users" {"name" "Alice" "age" 30 "role" "admin"})]
      (let [record (scriva/find-by-id db "users" id)]
        (println (get-in record [:data "name"]))         ; Alice
        (println (:id record) "rev=" (:rev record)))

      ;; find-records returns a lazy seq of record maps
      (doseq [r (scriva/find-records db "users"
                                     :filter {:field "role" :op :eq :value "admin"})]
        (println r))

      (scriva/update-record db "users" id {"name" "Alice" "age" 31})
      (scriva/delete db "users" id))

    (scriva/drop-collection db "users")
    (finally (scriva/close db))))
```

---

## API reference

### Connecting

```clojure
;; Plaintext
(def db (scriva/connect {:host "localhost" :port 5433 :api-key "dev-key"}))

;; TLS — verifies the server against the supplied CA certificate (PEM path)
(def db (scriva/connect {:host "host" :port 5433 :api-key "dev-key"
                         :tls-ca-cert "/path/ca.crt"}))

(scriva/close db)
```

`x-api-key` is attached as gRPC metadata on every call.

### Records

Reads return maps shaped like:

```clojure
{:id 1 :key "" :rev 1
 :data {"name" "Alice" "age" 30.0}     ; string keys, values decoded
 :date-added "2026-..." :date-modified nil}
```

### Collections, CRUD & TTL

```clojure
(scriva/create-collection db "col")
(scriva/create-collection db "sessions" 3600)          ; default per-record TTL (seconds)
(scriva/drop-collection db "col")
(scriva/list-collections db)

(scriva/insert db "col" {"field" "value"})
(scriva/insert db "sessions" {"token" "abc"} :ttl-seconds 60)
(scriva/insert-many db "col" [{"name" "Alice"} {"name" "Bob"}])
(scriva/update-record db "col" id {"name" "new"})
(scriva/delete db "col" id)
```

### Keyed CRUD, upsert & compare-and-swap (N1)

```clojure
(scriva/insert-keyed db "users" "user:42" {"name" "Alice"})  ; throws :already-exists if taken
(scriva/upsert db "users" "user:42" {"plan" "pro"})
(scriva/find-by-key db "users" "user:42")                    ; throws :not-found if absent
(scriva/update-by-key db "users" "user:42" {"plan" "team"})
(scriva/delete-by-key db "users" "user:42")

(let [{:keys [swapped record]} (scriva/update-if-rev db "users" "user:42" 2 {"plan" "enterprise"})]
  (when swapped (println (:rev record))))
```

Errors surface as `ex-info` with `{:type :not-found}` / `{:type :already-exists}`
in the ex-data (plus the original `:grpc-code`).

### Ordering, projection & keyset pagination (N2/N3)

```clojure
(scriva/find-by-id db "users" id :fields ["name" "email"])

(loop [token ""]
  (let [page (scriva/find-page db "scores" :limit 50
                               :order-by [{:field "team"} {:field "score" :desc true}]
                               :page-token token)]
    (doseq [r (:records page)] ...)
    (when (seq (:next-page-token page)) (recur (:next-page-token page)))))
```

### Aggregations (N4)

```clojure
(scriva/count-records db "orders")
(scriva/count-records db "orders" :filter {:field "status" :op :eq :value "shipped"})

(doseq [g (scriva/group-by-field db "orders" "region" [:sum :avg :min :max] "total")]
  (println (:group g) "count=" (:count g) "sum=" (:sum g)))
```

### Watch (streaming change feed)

```clojure
;; a lazy seq that blocks for each event; consume it on another thread
(future
  (doseq [event (scriva/watch db "col")]
    (println (:op event) "id=" (get-in event [:record :id]))))
```

### Indexes, transactions, stats & maintenance

```clojure
(scriva/ensure-index db "col" "name")
(scriva/drop-index db "col" "name")
(scriva/list-indexes db "col")

(let [tx (scriva/begin-tx db "col")] (scriva/commit-tx db tx) (scriva/rollback-tx db tx))

(scriva/stats db "col")           ; {:collection :record-count :segment-count :dirty-entries :size-bytes}
(scriva/compact db "col")
(scriva/snapshot-to-file db "backup.tar.gz")
```

---

## Filters

Filters are plain Clojure data, mirroring the other SDKs:

```clojure
{:field "age" :op :gt :value 30}

{:and [{:field "age"  :op :gte :value 18}
       {:field "name" :op :contains :value "alice"}]}

{:or [{:field "status" :op :eq :value "active"}
      {:field "role"   :op :eq :value "admin"}]}
```

`:op` values: `:eq :neq :gt :gte :lt :lte :contains :regex`.

---

## Running the example

```bash
# From the repo root, start a server:
make run
# Then:
cd clients/clojure
SCRIVA_API_KEY=dev-key lein run -m scriva.example
```

---

## Running the tests

Tests are hermetic — an in-process gRPC server is started per run, so no
external ScrivaDB is required:

```bash
cd clients/clojure
lein test
```
