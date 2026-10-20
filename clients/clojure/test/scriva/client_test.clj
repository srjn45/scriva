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
      (.onCompleted resp))
    (beginXTx [req resp]
      (.onNext resp (-> (scriva.v1.ScrivaOuterClass$BeginXTxResponse/newBuilder)
                        (.setXtxId "xtx-1") (.addAllCollections (sort (.getCollectionsList req)))
                        (.setIdempotencyKey (.getIdempotencyKey req)) (.build)))
      (.onCompleted resp))
    (stageXTx [req resp]
      (reset! (:last-stage st) req)
      (let [response (-> (scriva.v1.ScrivaOuterClass$StageXTxResponse/newBuilder)
                         (.setId (.getId req)) (.build))]
        (.onNext resp response)
        (.onCompleted resp)))
    (getXTx [req resp]
      (let [record (-> (scriva.v1.ScrivaOuterClass$Record/newBuilder)
                       (.setId (.getId req)) (.setRev 3) (.build))
            response (-> (scriva.v1.ScrivaOuterClass$GetXTxResponse/newBuilder)
                         (.setRecord record) (.build))]
        (.onNext resp response)
        (.onCompleted resp)))
    (commitXTx [req resp]
      (swap! (:commit-calls st) inc)
      (if @(:commit-error st)
        (let [info (-> (com.google.rpc.ErrorInfo/newBuilder) (.setDomain "scriva.xtx")
                       (.setReason "XTX_OUTCOME_UNKNOWN") (.putMetadata "status_ref" "idem-unknown")
                       (.putMetadata "retry_safe" "false") (.build))
              status (-> (com.google.rpc.Status/newBuilder) (.setCode (.value io.grpc.Status$Code/UNKNOWN))
                         (.addDetails (com.google.protobuf.Any/pack info)) (.build))]
          (reset! (:commit-error st) false)
          (.onError resp (io.grpc.protobuf.StatusProto/toStatusRuntimeException status)))
        (let [op (-> (scriva.v1.ScrivaOuterClass$XTxOpResult/newBuilder)
                     (.setCollection "accounts") (.setId 8) (.setRev 4)
                     (.setOp scriva.v1.ScrivaOuterClass$XTxOpKind/XTX_OP_UPDATE) (.build))
              response (-> (scriva.v1.ScrivaOuterClass$CommitXTxResponse/newBuilder)
                           (.setTxId "decision-1") (.addOps op) (.build))]
          (.onNext resp response)
          (.onCompleted resp))))
    (rollbackXTx [req resp]
      (let [response (-> (scriva.v1.ScrivaOuterClass$RollbackXTxResponse/newBuilder) (.setOk true) (.build))]
        (.onNext resp response)
        (.onCompleted resp)))
    (xTxStatus [req resp]
      (swap! (:status-calls st) inc)
      (reset! (:status-ref st) (.getRef req))
      (let [response (-> (scriva.v1.ScrivaOuterClass$XTxStatusResponse/newBuilder)
                         (.setStatus scriva.v1.ScrivaOuterClass$XTxState/XTX_STATUS_COMMITTED)
                         (.setTxId "decision-1") (.build))]
        (.onNext resp response)
        (.onCompleted resp)))))

(use-fixtures :once
  (fn [f]
    (let [st {:ids (atom 0) :store (atom {}) :last-filter (atom nil) :last-stage (atom nil) :commit-calls (atom 0)
              :commit-error (atom false) :status-calls (atom 0) :status-ref (atom nil)}
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

(deftest xtx-wrappers-preserve-wire-fields-and-commit-once
  (let [before @(:commit-calls @state)
        tx (scriva/begin-xtx @db ["ledger" "accounts"] :idempotency-key "key-1" :idle-timeout-ms 20 :max-lifetime-ms 40)]
    (is (= {:xtx-id "xtx-1" :collections ["accounts" "ledger"] :idempotency-key "key-1"} tx))
    (is (= 8 (scriva/stage-xtx @db (:xtx-id tx) {:collection "accounts" :op :update :id 8 :data {"balance" 9} :expected-rev 3})))
    (is (= 3 (:rev (scriva/get-xtx @db (:xtx-id tx) "accounts" 8))))
    (is (= "decision-1" (:tx-id (scriva/commit-xtx @db (:xtx-id tx) :lock-timeout-ms 50))))
    (is (scriva/rollback-xtx @db (:xtx-id tx)))
    (is (= {:status :xtx_status_committed :tx-id "decision-1"} (scriva/xtx-status @db "key-1")))
    (is (= (inc before) @(:commit-calls @state)) "no blind commit retry")))

(deftest unknown-commit-outcome-decodes-errorinfo-and-resolves-status
  (let [before-commits @(:commit-calls @state)
        before-status @(:status-calls @state)
        tx (scriva/begin-xtx @db ["a" "b"] :idempotency-key "idem-unknown")]
    (reset! (:commit-error @state) true)
    (let [error (try (scriva/commit-xtx @db (:xtx-id tx)) nil
                     (catch clojure.lang.ExceptionInfo e e))
          detail (scriva/xtx-error-detail error)]
      (is (= "XTX_OUTCOME_UNKNOWN" (:reason detail)))
      (is (= "idem-unknown" (get-in detail [:metadata "status_ref"])))
      (is (= "false" (get-in detail [:metadata "retry_safe"])))
      (is (= :xtx_status_committed (:status (scriva/xtx-status @db (get-in detail [:metadata "status_ref"])))))
      (is (= (inc before-commits) @(:commit-calls @state)))
      (is (= (inc before-status) @(:status-calls @state)))
      (is (= "idem-unknown" @(:status-ref @state))))))
