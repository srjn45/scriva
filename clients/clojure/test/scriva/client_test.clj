(ns scriva.client-test
  "Hermetic tests: an in-process gRPC server backed by an in-memory fake exercises
  client construction, message round-tripping and streaming — no external server,
  no TCP ports."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [scriva.client :as scriva])
  (:import [io.grpc.inprocess InProcessServerBuilder InProcessChannelBuilder]
           [io.grpc Status]))

(def ^:private state (atom nil))
(def ^:private db (atom nil))
(def ^:private server (atom nil))
(def ^:private channel (atom nil))

(defn- make-fake [st]
  (proxy [scriva.v1.ScrivaGrpc$ScrivaImplBase] []
    (createCollection [req resp]
      (.onNext resp (-> (scriva.v1.ScrivaOuterClass$CreateCollectionResponse/newBuilder)
                        (.setName (.getName req)) (.build)))
      (.onCompleted resp))
    (insert [req resp]
      (let [id (swap! (:ids st) inc)
            rec (-> (scriva.v1.ScrivaOuterClass$Record/newBuilder)
                    (.setId (long id)) (.setRev 1) (.setData (.getData req)) (.build))]
        (swap! (:store st) update (.getCollection req) (fnil conj []) rec)
        (.onNext resp (-> (scriva.v1.ScrivaOuterClass$InsertResponse/newBuilder)
                          (.setId (long id)) (.setRev 1) (.build)))
        (.onCompleted resp)))
    (findById [req resp]
      (if-let [rec (some #(when (= (.getId %) (.getId req)) %)
                         (get @(:store st) (.getCollection req)))]
        (do (.onNext resp (-> (scriva.v1.ScrivaOuterClass$FindResponse/newBuilder)
                              (.setRecord rec) (.build)))
            (.onCompleted resp))
        (.onError resp (.asRuntimeException (.withDescription Status/NOT_FOUND "no such id")))))
    (find [req resp]
      (reset! (:last-filter st) (when (.hasFilter req) (.getFilter req)))
      (doseq [rec (get @(:store st) (.getCollection req))]
        (.onNext resp (-> (scriva.v1.ScrivaOuterClass$FindResponse/newBuilder)
                          (.setRecord rec) (.build))))
      (.onCompleted resp))
    (findByKey [req resp]
      (.onError resp (.asRuntimeException (.withDescription Status/NOT_FOUND "no such key"))))
    (watch [req resp]
      (.onNext resp (-> (scriva.v1.ScrivaOuterClass$WatchEvent/newBuilder)
                        (.setOp scriva.v1.ScrivaOuterClass$WatchOp/INSERTED)
                        (.setCollection (.getCollection req))
                        (.setRecord (-> (scriva.v1.ScrivaOuterClass$Record/newBuilder)
                                        (.setId 1) (.setRev 1) (.build)))
                        (.build)))
      (.onNext resp (-> (scriva.v1.ScrivaOuterClass$WatchEvent/newBuilder)
                        (.setOp scriva.v1.ScrivaOuterClass$WatchOp/DELETED)
                        (.setCollection (.getCollection req)) (.build)))
      (.onCompleted resp))))

(use-fixtures :once
  (fn [f]
    (let [st {:ids (atom 0) :store (atom {}) :last-filter (atom nil)}
          nm (str (gensym "scriva-test"))
          srv (-> (InProcessServerBuilder/forName nm) (.directExecutor)
                  (.addService (make-fake st)) (.build) (.start))
          ch  (-> (InProcessChannelBuilder/forName nm) (.directExecutor) (.build))]
      (reset! state st)
      (reset! server srv)
      (reset! channel ch)
      (reset! db (scriva/connect {:channel ch :api-key "test-key"}))
      (try (f)
        (finally
          (scriva/close @db)
          (.shutdownNow ^io.grpc.ManagedChannel ch)
          (.shutdownNow srv))))))

(deftest struct-round-trip
  (testing "nested values survive a Struct round-trip"
    (let [m {"name" "Alice" "age" 30.0 "active" true
             "tags" ["a" "b"] "nested" {"x" 1.0 "y" [true false]}}]
      (is (= m (#'scriva.client/struct-> (#'scriva.client/->struct m)))))))

(deftest create-insert-find-by-id
  (testing "client construction + unary round-trip"
    (is (= "users" (scriva/create-collection @db "users")))
    (let [id (scriva/insert @db "users" {"name" "Alice" "age" 30})]
      (is (pos? id))
      (let [r (scriva/find-by-id @db "users" id)]
        (is (= "Alice" (get-in r [:data "name"])))
        (is (= 30.0 (get-in r [:data "age"])))
        (is (= id (:id r)))
        (is (= 1 (:rev r)))))))

(deftest find-streams-and-converts-filter
  (testing "find streams records as a lazy seq and converts the filter"
    (scriva/create-collection @db "people")
    (scriva/insert @db "people" {"name" "Alice" "role" "admin"})
    (scriva/insert @db "people" {"name" "Bob" "role" "user"})
    (let [records (doall (scriva/find-records @db "people"
                                              :filter {:and [{:field "age" :op :gt :value 18}
                                                             {:field "role" :op :eq :value "admin"}]}))
          f @(:last-filter @state)]
      (is (= 2 (count records)))
      (is (.hasAnd f))
      (is (= "age" (.. f getAnd (getFilters 0) getField getField)))
      (is (= scriva.v1.ScrivaOuterClass$FilterOp/GT
             (.. f getAnd (getFilters 0) getField getOp))))))

(deftest watch-maps-events
  (testing "watch yields mapped event maps as a lazy seq"
    (let [events (doall (take 2 (scriva/watch @db "people")))]
      (is (= [:inserted :deleted] (map :op events)))
      (is (nil? (:record (second events)))))))

(deftest not-found-translated
  (testing "NOT_FOUND surfaces as an ex-info with :type :not-found"
    (let [ex (try (scriva/find-by-key @db "people" "ghost") nil
                  (catch clojure.lang.ExceptionInfo e e))]
      (is (some? ex))
      (is (= :not-found (:type (ex-data ex))))
      (is (re-find #"no such key" (.getMessage ex))))))
