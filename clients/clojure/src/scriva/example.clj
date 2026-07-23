(ns scriva.example
  "End-to-end example mirroring the Java client's BasicExample: connect, manage
  collections, CRUD, keyed CRUD + CAS (N1), keyset pagination + multi-field
  ordering (N3), aggregations (N4), and a lazy-seq watch.

  Requires a running server (`make run` from the repo root). Run with:
    lein run -m scriva.example"
  (:require [scriva.client :as scriva]))

(defn -main [& _]
  (let [host   (or (System/getenv "SCRIVA_HOST") "localhost")
        port   (Integer/parseInt (or (System/getenv "SCRIVA_PORT") "5433"))
        api-key (or (System/getenv "SCRIVA_API_KEY") "dev-key")
        db (scriva/connect {:host host :port port :api-key api-key})
        col "test_clojure"]
    (try
      (println "=== Collection management ===")
      (println "Created:" (scriva/create-collection db col))
      (scriva/ensure-index db col "name")
      (println "Indexes:" (scriva/list-indexes db col))

      (println "\n=== Insert ===")
      (let [id1 (scriva/insert db col {"name" "Alice" "age" 30 "role" "admin"})]
        (scriva/insert-many db col [{"name" "Bob" "age" 25 "role" "user"}
                                    {"name" "Carol" "age" 35 "role" "user"}])
        (println "Inserted id1=" id1)

        (println "\n=== FindById ===")
        (println (scriva/find-by-id db col id1)))

      (println "\n=== Find (age > 25 AND role = user) ===")
      (doseq [r (scriva/find-records db col
                                     :filter {:and [{:field "age" :op :gt :value 25}
                                                    {:field "role" :op :eq :value "user"}]})]
        (println "  " r))

      (println "\n=== Keyed CRUD + CAS (N1) ===")
      (let [kcol (str col "_k")]
        (scriva/create-collection db kcol)
        (scriva/upsert db kcol "user:1" {"plan" "free"})
        (let [replaced (scriva/upsert db kcol "user:1" {"plan" "pro"})
              cas (scriva/update-if-rev db kcol "user:1" (:rev replaced) {"plan" "enterprise"})]
          (println "CAS swapped=" (:swapped cas) "->" (:record cas)))
        (scriva/drop-collection db kcol))

      (println "\n=== Keyset pagination + multi-field order (N3) ===")
      (let [pcol (str col "_p")]
        (scriva/create-collection db pcol)
        (scriva/insert-many db pcol [{"team" "red" "score" 10}
                                     {"team" "blue" "score" 20}
                                     {"team" "red" "score" 30}])
        (loop [token ""]
          (let [page (scriva/find-page db pcol :limit 2
                                       :order-by [{:field "team"} {:field "score" :desc true}]
                                       :page-token token)]
            (doseq [r (:records page)]
              (println "  " (get-in r [:data "team"]) (get-in r [:data "score"])))
            (when (seq (:next-page-token page)) (recur (:next-page-token page)))))
        (println "count(all in _p) =" (scriva/count-records db pcol))
        (doseq [g (scriva/group-by-field db pcol "team" [:sum :avg] "score")]
          (println "  " (:group g) "-> count=" (:count g) "sum=" (:sum g)))
        (scriva/drop-collection db pcol))

      (println "\n=== Watch ===")
      (let [wcol (str col "_w")]
        (scriva/create-collection db wcol)
        (let [events (future (doall (take 2 (scriva/watch db wcol))))]
          (Thread/sleep 200)
          (scriva/insert db wcol {"event" "first"})
          (scriva/insert db wcol {"event" "second"})
          (doseq [e @events] (println "  " (:op e) "id=" (get-in e [:record :id]))))
        (scriva/drop-collection db wcol))

      (scriva/drop-collection db col)
      (println "\nDone.")
      (finally (scriva/close db)))))
