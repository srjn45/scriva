(ns scriva.client
  "Idiomatic Clojure client for ScrivaDB.

  Functions take and return plain Clojure maps; streaming RPCs (find, watch,
  aggregate, snapshot) return lazy seqs. Every call carries the `x-api-key`
  gRPC metadata automatically.

      (require '[scriva.client :as scriva])
      (let [db (scriva/connect {:host \"localhost\" :port 5433 :api-key \"dev-key\"})]
        (scriva/create-collection db \"users\")
        (let [id (scriva/insert db \"users\" {\"name\" \"Alice\" \"age\" 30})]
          (println (get-in (scriva/find-by-id db \"users\" id) [:data \"name\"])))
        (scriva/close db))"
  (:import [com.google.protobuf Struct Value ListValue NullValue Timestamp]
           [io.grpc ManagedChannel ManagedChannelBuilder Metadata Metadata$Key
            ClientInterceptor ClientInterceptors Status Status$Code StatusRuntimeException]
           [com.google.rpc ErrorInfo]
           [io.grpc.stub MetadataUtils]
           [io.grpc.netty.shaded.io.grpc.netty NettyChannelBuilder GrpcSslContexts]
           [java.io File FileOutputStream BufferedOutputStream]
           [java.time Instant]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* false)

;; ── Struct <-> Clojure data ────────────────────────────────────────────────

(declare ->struct)

(defn- ->value ^Value [v]
  (let [b (Value/newBuilder)]
    (cond
      (nil? v)          (.setNullValue b NullValue/NULL_VALUE)
      (boolean? v)      (.setBoolValue b v)
      (number? v)       (.setNumberValue b (double v))
      (string? v)       (.setStringValue b v)
      (keyword? v)      (.setStringValue b (name v))
      (map? v)          (.setStructValue b (->struct v))
      (sequential? v)   (.setListValue b (let [lb (ListValue/newBuilder)]
                                           (doseq [x v] (.addValues lb (->value x)))
                                           (.build lb)))
      :else             (.setStringValue b (str v)))
    (.build b)))

(defn- ->struct ^Struct [m]
  (let [b (Struct/newBuilder)]
    (doseq [[k v] m] (.putFields b (name k) (->value v)))
    (.build b)))

(defn- value-> [^Value v]
  (condp = (.name (.getKindCase v))
    "NULL_VALUE"   nil
    "BOOL_VALUE"   (.getBoolValue v)
    "NUMBER_VALUE" (.getNumberValue v)
    "STRING_VALUE" (.getStringValue v)
    "STRUCT_VALUE" (into {} (map (fn [[k vv]] [k (value-> vv)]) (.getFieldsMap (.getStructValue v))))
    "LIST_VALUE"   (mapv value-> (.getValuesList (.getListValue v)))
    nil))

(defn- struct-> [^Struct s]
  (into {} (map (fn [[k v]] [k (value-> v)]) (.getFieldsMap s))))

(defn- ts-> [^Timestamp ts]
  (str (Instant/ofEpochSecond (.getSeconds ts) (.getNanos ts))))

(defn- record-> [r]
  {:id            (.getId r)
   :key           (.getKey r)
   :rev           (.getRev r)
   :data          (if (.hasData r) (struct-> (.getData r)) {})
   :date-added    (when (.hasDateAdded r) (ts-> (.getDateAdded r)))
   :date-modified (when (.hasDateModified r) (ts-> (.getDateModified r)))})

;; ── Filters (plain-data → proto) ───────────────────────────────────────────

(def ^:private ops
  {:eq scriva.v1.ScrivaOuterClass$FilterOp/EQ
   :neq scriva.v1.ScrivaOuterClass$FilterOp/NEQ
   :gt scriva.v1.ScrivaOuterClass$FilterOp/GT
   :gte scriva.v1.ScrivaOuterClass$FilterOp/GTE
   :lt scriva.v1.ScrivaOuterClass$FilterOp/LT
   :lte scriva.v1.ScrivaOuterClass$FilterOp/LTE
   :contains scriva.v1.ScrivaOuterClass$FilterOp/CONTAINS
   :regex scriva.v1.ScrivaOuterClass$FilterOp/REGEX})

(declare ->filter)

(defn- ->filter [f]
  (cond
    (:and f) (let [b (scriva.v1.ScrivaOuterClass$AndFilter/newBuilder)]
               (doseq [c (:and f)] (.addFilters b (->filter c)))
               (-> (scriva.v1.ScrivaOuterClass$Filter/newBuilder) (.setAnd (.build b)) (.build)))
    (:or f)  (let [b (scriva.v1.ScrivaOuterClass$OrFilter/newBuilder)]
               (doseq [c (:or f)] (.addFilters b (->filter c)))
               (-> (scriva.v1.ScrivaOuterClass$Filter/newBuilder) (.setOr (.build b)) (.build)))
    :else    (let [op (get ops (keyword (:op f)))
                   ff (-> (scriva.v1.ScrivaOuterClass$FieldFilter/newBuilder)
                          (.setField (:field f))
                          (.setOp op)
                          (.setValue (str (:value f)))
                          (.build))]
               (-> (scriva.v1.ScrivaOuterClass$Filter/newBuilder) (.setField ff) (.build)))))

(def ^:private aggs
  {:count scriva.v1.ScrivaOuterClass$AggregateOp/AGG_COUNT
   :sum scriva.v1.ScrivaOuterClass$AggregateOp/AGG_SUM
   :avg scriva.v1.ScrivaOuterClass$AggregateOp/AGG_AVG
   :min scriva.v1.ScrivaOuterClass$AggregateOp/AGG_MIN
   :max scriva.v1.ScrivaOuterClass$AggregateOp/AGG_MAX})

;; ── Errors ─────────────────────────────────────────────────────────────────

(defn- call
  "Invoke `f`, translating gRPC status errors into ex-info with a :type."
  [f]
  (try
    (f)
    (catch StatusRuntimeException e
      (let [code (.. e getStatus getCode)
            msg  (or (.. e getStatus getDescription) (.getMessage e))
            type (condp = code
                   Status$Code/NOT_FOUND :not-found
                   Status$Code/ALREADY_EXISTS :already-exists
                   :error)]
        ;; Keep the original status (and trailers, where ErrorInfo lives) available
        ;; to callers resolving an XTx outcome; do not infer retry safety from text.
        (throw (ex-info msg {:type type :grpc-code (str code) :grpc-cause e
                            :grpc-trailers (.getTrailers e)} e))))))

(defn xtx-error-detail
  "Decode scriva.xtx ErrorInfo from a translated gRPC exception. Callers resolve
  XTX_OUTCOME_UNKNOWN with :status_ref; this helper never retries commit."
  [error]
  (let [{:keys [grpc-cause grpc-code grpc-trailers]} (ex-data error)
        status (when grpc-cause (io.grpc.protobuf.StatusProto/fromThrowable grpc-cause))
        info (some (fn [detail]
                     (try
                       (when (.is detail ErrorInfo)
                         (let [unpacked (.unpack detail ErrorInfo)]
                           (when (= "scriva.xtx" (.getDomain unpacked)) unpacked)))
                       (catch com.google.protobuf.InvalidProtocolBufferException _ nil)))
                   (when status (.getDetailsList status)))]
    (cond-> {:grpc-code grpc-code :grpc-cause grpc-cause :grpc-trailers grpc-trailers}
      info (assoc :reason (.getReason info) :metadata (into {} (.getMetadataMap info))))))

;; ── Connection ─────────────────────────────────────────────────────────────

(def ^:private ^Metadata$Key api-key-header
  (Metadata$Key/of "x-api-key" Metadata/ASCII_STRING_MARSHALLER))

(defn- ->client [^ManagedChannel channel api-key]
  (let [md (doto (Metadata.) (.put api-key-header api-key))
        interceptor (MetadataUtils/newAttachHeadersInterceptor md)
        intercepted (ClientInterceptors/intercept channel ^"[Lio.grpc.ClientInterceptor;"
                                                   (into-array ClientInterceptor [interceptor]))]
    {:channel  channel
     :blocking (scriva.v1.ScrivaGrpc/newBlockingStub intercepted)}))

(defn connect
  "Open a client. Opts: :host :port :api-key, optional :tls-ca-cert (path to a
  PEM CA cert; plaintext when omitted). A prebuilt :channel may be supplied
  instead of host/port (used by tests)."
  [{:keys [host port api-key tls-ca-cert channel]}]
  (->client
    (or channel
        (if tls-ca-cert
          (-> (NettyChannelBuilder/forAddress host (int port))
              (.sslContext (-> (GrpcSslContexts/forClient)
                               (.trustManager (File. ^String tls-ca-cert))
                               (.build)))
              (.build))
          (-> (ManagedChannelBuilder/forAddress host (int port)) (.usePlaintext) (.build))))
    api-key))

(defn close
  "Shut the client's channel down."
  [{:keys [^ManagedChannel channel]}]
  (.awaitTermination (.shutdown channel) 5 TimeUnit/SECONDS))

(defn- stub [client] (:blocking client))

;; ── Collection management ──────────────────────────────────────────────────

(defn create-collection
  ([client name] (create-collection client name 0))
  ([client name default-ttl-seconds]
   (-> (call #(.createCollection (stub client)
                                 (-> (scriva.v1.ScrivaOuterClass$CreateCollectionRequest/newBuilder)
                                     (.setName name)
                                     (.setDefaultTtlSeconds (long default-ttl-seconds))
                                     (.build))))
       (.getName))))

(defn drop-collection [client name]
  (-> (call #(.dropCollection (stub client)
                              (-> (scriva.v1.ScrivaOuterClass$DropCollectionRequest/newBuilder)
                                  (.setName name) (.build))))
      (.getOk)))

(defn list-collections [client]
  (-> (call #(.listCollections (stub client)
                               (.build (scriva.v1.ScrivaOuterClass$ListCollectionsRequest/newBuilder))))
      (.getNamesList) (vec)))

;; ── CRUD ───────────────────────────────────────────────────────────────────

(defn insert
  "Insert one record. Opts: :ttl-seconds, :key (caller-supplied primary key)."
  [client collection data & {:keys [ttl-seconds key] :or {ttl-seconds 0 key ""}}]
  (-> (call #(.insert (stub client)
                      (-> (scriva.v1.ScrivaOuterClass$InsertRequest/newBuilder)
                          (.setCollection collection)
                          (.setData (->struct data))
                          (.setTtlSeconds (long ttl-seconds))
                          (.setKey key)
                          (.build))))
      (.getId)))

(defn insert-keyed
  "Keyed insert under `key`; throws :already-exists if taken."
  [client collection key data]
  (insert client collection data :key key))

(defn insert-many
  [client collection records & {:keys [ttl-seconds] :or {ttl-seconds 0}}]
  (let [b (-> (scriva.v1.ScrivaOuterClass$InsertManyRequest/newBuilder)
              (.setCollection collection)
              (.setTtlSeconds (long ttl-seconds)))]
    (doseq [r records] (.addRecords b (->struct r)))
    (-> (call #(.insertMany (stub client) (.build b))) (.getIdsList) (vec))))

(defn find-by-id
  [client collection id & {:keys [fields]}]
  (let [b (-> (scriva.v1.ScrivaOuterClass$FindByIdRequest/newBuilder)
              (.setCollection collection)
              (.setId (long id)))]
    (when (seq fields) (.addAllFields b (map name fields)))
    (record-> (.getRecord (call #(.findById (stub client) (.build b)))))))

(defn- find-request
  [collection {:keys [filter limit offset order-by fields page-token]
               :or {limit 0 offset 0 page-token ""}}]
  (let [b (-> (scriva.v1.ScrivaOuterClass$FindRequest/newBuilder)
              (.setCollection collection)
              (.setLimit (int limit))
              (.setOffset (int offset))
              (.setPageToken page-token))]
    (when filter (.setFilter b (->filter filter)))
    (when (seq fields) (.addAllFields b (map name fields)))
    (doseq [o order-by]
      (.addOrderByFields b (-> (scriva.v1.ScrivaOuterClass$OrderBy/newBuilder)
                               (.setField (:field o))
                               (.setDesc (boolean (:desc o)))
                               (.build))))
    (.build b)))

(defn find-records
  "Stream matching records as a lazy seq of record maps. Opts: :filter :limit
  :offset :order-by (seq of {:field :desc}) :fields :page-token."
  [client collection & {:as opts}]
  (->> (.find (stub client) (find-request collection opts))
       (iterator-seq)
       (map #(record-> (.getRecord %)))))

(defn find-page
  "Fetch one keyset page (N3): {:records [...] :next-page-token \"...\"}."
  [client collection & {:as opts}]
  (let [resps (doall (iterator-seq (.find (stub client) (find-request collection opts))))]
    {:records (mapv #(record-> (.getRecord %)) resps)
     :next-page-token (or (->> resps (map #(.getPageToken %)) (filter seq) last) "")}))

(defn update-record
  [client collection id data & {:keys [ttl-seconds] :or {ttl-seconds 0}}]
  (-> (call #(.update (stub client)
                      (-> (scriva.v1.ScrivaOuterClass$UpdateRequest/newBuilder)
                          (.setCollection collection)
                          (.setId (long id))
                          (.setData (->struct data))
                          (.setTtlSeconds (long ttl-seconds))
                          (.build))))
      (.getId)))

(defn delete
  [client collection id]
  (-> (call #(.delete (stub client)
                      (-> (scriva.v1.ScrivaOuterClass$DeleteRequest/newBuilder)
                          (.setCollection collection) (.setId (long id)) (.build))))
      (.getOk)))

;; ── Keyed CRUD, upsert & compare-and-swap (N1) ─────────────────────────────

(defn upsert
  [client collection key data]
  (record-> (.getRecord (call #(.upsert (stub client)
                                        (-> (scriva.v1.ScrivaOuterClass$UpsertRequest/newBuilder)
                                            (.setCollection collection)
                                            (.setKey key)
                                            (.setData (->struct data))
                                            (.build)))))))

(defn find-by-key
  [client collection key & {:keys [fields]}]
  (let [b (-> (scriva.v1.ScrivaOuterClass$FindByKeyRequest/newBuilder)
              (.setCollection collection)
              (.setKey key))]
    (when (seq fields) (.addAllFields b (map name fields)))
    (record-> (.getRecord (call #(.findByKey (stub client) (.build b)))))))

(defn update-by-key
  [client collection key data]
  (let [r (call #(.updateByKey (stub client)
                               (-> (scriva.v1.ScrivaOuterClass$UpdateByKeyRequest/newBuilder)
                                   (.setCollection collection)
                                   (.setKey key)
                                   (.setData (->struct data))
                                   (.build))))]
    {:id (.getId r) :key (.getKey r) :rev (.getRev r) :date-modified (.getDateModified r)}))

(defn delete-by-key
  [client collection key]
  (-> (call #(.deleteByKey (stub client)
                           (-> (scriva.v1.ScrivaOuterClass$DeleteByKeyRequest/newBuilder)
                               (.setCollection collection) (.setKey key) (.build))))
      (.getOk)))

(defn update-if-rev
  "Compare-and-swap on `key`, conditional on `expected-rev`. Returns
  {:swapped bool :record {...}|nil}; a stale rev (or missing key) is a no-op."
  [client collection key expected-rev data]
  (let [r (call #(.updateIfRev (stub client)
                               (-> (scriva.v1.ScrivaOuterClass$UpdateIfRevRequest/newBuilder)
                                   (.setCollection collection)
                                   (.setKey key)
                                   (.setExpectedRev (long expected-rev))
                                   (.setData (->struct data))
                                   (.build))))]
    {:swapped (.getSwapped r)
     :record (when (and (.getSwapped r) (.hasRecord r)) (record-> (.getRecord r)))}))

;; ── Secondary indexes ──────────────────────────────────────────────────────

(defn ensure-index [client collection field]
  (call #(.ensureIndex (stub client)
                       (-> (scriva.v1.ScrivaOuterClass$EnsureIndexRequest/newBuilder)
                           (.setCollection collection) (.setField field) (.build))))
  nil)

(defn drop-index [client collection field]
  (-> (call #(.dropIndex (stub client)
                         (-> (scriva.v1.ScrivaOuterClass$DropIndexRequest/newBuilder)
                             (.setCollection collection) (.setField field) (.build))))
      (.getOk)))

(defn list-indexes [client collection]
  (-> (call #(.listIndexes (stub client)
                           (-> (scriva.v1.ScrivaOuterClass$ListIndexesRequest/newBuilder)
                               (.setCollection collection) (.build))))
      (.getFieldsList) (vec)))

;; ── Transactions ───────────────────────────────────────────────────────────

(def ^:private xtx-ops
  {:insert scriva.v1.ScrivaOuterClass$XTxOpKind/XTX_OP_INSERT
   :update scriva.v1.ScrivaOuterClass$XTxOpKind/XTX_OP_UPDATE
   :delete scriva.v1.ScrivaOuterClass$XTxOpKind/XTX_OP_DELETE})

(defn begin-xtx
  "Open a cross-collection transaction. Options: :idempotency-key :idle-timeout-ms :max-lifetime-ms. Retain an explicit key for recoverable commits."
  [client collections & {:keys [idempotency-key idle-timeout-ms max-lifetime-ms]
                         :or {idempotency-key "" idle-timeout-ms 0 max-lifetime-ms 0}}]
  (let [r (call #(.beginXTx (stub client)
                             (-> (scriva.v1.ScrivaOuterClass$BeginXTxRequest/newBuilder)
                                 (.addAllCollections collections) (.setIdempotencyKey idempotency-key)
                                 (.setIdleTimeoutMs (long idle-timeout-ms)) (.setMaxLifetimeMs (long max-lifetime-ms)) (.build))))]
    {:xtx-id (.getXtxId r) :collections (vec (.getCollectionsList r)) :idempotency-key (.getIdempotencyKey r)}))

(defn stage-xtx
  "Stage {:collection :op (:insert/:update/:delete) :id :data :expected-rev}; returns a reserved id for inserts."
  [client xtx-id {:keys [collection op id data expected-rev] :or {id 0 expected-rev 0}}]
  (let [kind (get xtx-ops op)]
    (when-not kind (throw (IllegalArgumentException. "op must be :insert, :update, or :delete")))
    (let [b (-> (scriva.v1.ScrivaOuterClass$StageXTxRequest/newBuilder)
                (.setXtxId xtx-id) (.setCollection collection) (.setOp kind) (.setId (long id)) (.setExpectedRev (long expected-rev)))]
      (when (and (not= op :delete) (some? data)) (.setData b (->struct data)))
      (.getId (call #(.stageXTx (stub client) (.build b)))))))

(defn get-xtx [client xtx-id collection id]
  (record-> (.getRecord (call #(.getXTx (stub client)
                                (-> (scriva.v1.ScrivaOuterClass$GetXTxRequest/newBuilder)
                                    (.setXtxId xtx-id) (.setCollection collection) (.setId (long id)) (.build)))))))

(defn commit-xtx
  "Commit once. For an unknown/in-progress outcome, call xtx-status; never blindly retry this call."
  [client xtx-id & {:keys [lock-timeout-ms] :or {lock-timeout-ms 0}}]
  (let [r (call #(.commitXTx (stub client) (-> (scriva.v1.ScrivaOuterClass$CommitXTxRequest/newBuilder)
                                                 (.setXtxId xtx-id) (.setLockTimeoutMs (long lock-timeout-ms)) (.build))))]
    {:tx-id (.getTxId r) :replayed (.getReplayed r)
     :ops (mapv #(hash-map :collection (.getCollection %) :id (.getId %) :rev (.getRev %) :op (keyword (.toLowerCase (str (.getOp %))))) (.getOpsList r))}))

(defn rollback-xtx [client xtx-id]
  (.getOk (call #(.rollbackXTx (stub client) (-> (scriva.v1.ScrivaOuterClass$RollbackXTxRequest/newBuilder) (.setXtxId xtx-id) (.build))))))

(defn xtx-status [client ref]
  (let [r (call #(.xTxStatus (stub client) (-> (scriva.v1.ScrivaOuterClass$XTxStatusRequest/newBuilder) (.setRef ref) (.build))))]
    {:status (keyword (.toLowerCase (str (.getStatus r)))) :tx-id (.getTxId r)}))

(defn begin-tx [client collection]
  (-> (call #(.beginTx (stub client)
                       (-> (scriva.v1.ScrivaOuterClass$BeginTxRequest/newBuilder)
                           (.setCollection collection) (.build))))
      (.getTxId)))

(defn commit-tx [client tx-id]
  (-> (call #(.commitTx (stub client)
                        (-> (scriva.v1.ScrivaOuterClass$CommitTxRequest/newBuilder)
                            (.setTxId tx-id) (.build))))
      (.getOk)))

(defn rollback-tx [client tx-id]
  (-> (call #(.rollbackTx (stub client)
                          (-> (scriva.v1.ScrivaOuterClass$RollbackTxRequest/newBuilder)
                              (.setTxId tx-id) (.build))))
      (.getOk)))

;; ── Watch (server-streaming change feed) ───────────────────────────────────

(def ^:private watch-ops
  {"INSERTED" :inserted "UPDATED" :updated "DELETED" :deleted "OVERFLOW" :overflow})

(defn- watch-event-> [ev]
  {:op         (get watch-ops (.name (.getOp ev)) :unspecified)
   :collection (.getCollection ev)
   :record     (when (.hasRecord ev) (record-> (.getRecord ev)))
   :ts         (when (.hasTs ev) (ts-> (.getTs ev)))})

(defn watch
  "Subscribe to change events on `collection` as a lazy seq of event maps.
  The seq blocks until the next event and runs until the stream ends. Opts: :filter."
  [client collection & {:keys [filter]}]
  (let [b (-> (scriva.v1.ScrivaOuterClass$WatchRequest/newBuilder) (.setCollection collection))]
    (when filter (.setFilter b (->filter filter)))
    (->> (.watch (stub client) (.build b)) (iterator-seq) (map watch-event->))))

;; ── Aggregations (N4) ──────────────────────────────────────────────────────

(defn aggregate
  "Compute count + numeric aggregations. Opts: :aggregations (seq of
  :count/:sum/:avg/:min/:max) :field :group-by :filter. Returns a seq of
  {:group :count :numeric :sum :avg :min :max}."
  [client collection & {:keys [aggregations field group-by filter]
                        :or {aggregations [] field "" group-by ""}}]
  (let [b (-> (scriva.v1.ScrivaOuterClass$AggregateRequest/newBuilder)
              (.setCollection collection)
              (.setField field)
              (.setGroupBy group-by))]
    (when filter (.setFilter b (->filter filter)))
    (doseq [a aggregations]
      (.addAggregations b (or (get aggs (keyword a))
                              (throw (IllegalArgumentException.
                                       (str "unknown aggregation '" a "'"))))))
    (->> (.aggregate (stub client) (.build b))
         (iterator-seq)
         (mapv (fn [r]
                 {:group   (when (.hasGroupValue r) (value-> (.getGroupValue r)))
                  :count   (.getCount r)
                  :numeric (.getNumeric r)
                  :sum     (.getSum r) :avg (.getAvg r) :min (.getMin r) :max (.getMax r)})))))

(defn count-records
  "Count all live records, or those matching :filter."
  [client collection & {:keys [filter]}]
  (let [gs (aggregate client collection :filter filter)]
    (if (seq gs) (:count (first gs)) 0)))

(defn group-by-field
  "Group live records by `field` and aggregate `metric` per group."
  [client collection field aggregations metric & {:keys [filter]}]
  (aggregate client collection
             :aggregations aggregations :field metric :group-by field :filter filter))

;; ── Stats ──────────────────────────────────────────────────────────────────

(defn stats [client collection]
  (let [r (call #(.collectionStats (stub client)
                                   (-> (scriva.v1.ScrivaOuterClass$CollectionStatsRequest/newBuilder)
                                       (.setCollection collection) (.build))))]
    {:collection    (.getCollection r)
     :record-count  (.getRecordCount r)
     :segment-count (.getSegmentCount r)
     :dirty-entries (.getDirtyEntries r)
     :size-bytes    (.getSizeBytes r)}))

;; ── Maintenance ────────────────────────────────────────────────────────────

(defn compact [client collection]
  (-> (call #(.compact (stub client)
                       (-> (scriva.v1.ScrivaOuterClass$CompactRequest/newBuilder)
                           (.setCollection collection) (.build))))
      (.getOk)))

(defn snapshot
  "Lazily stream the gzip-tar snapshot archive as a seq of byte arrays."
  [client]
  (->> (.snapshot (stub client) (.build (scriva.v1.ScrivaOuterClass$SnapshotRequest/newBuilder)))
       (iterator-seq)
       (map #(.toByteArray (.getData %)))))

(defn snapshot-to-file
  "Stream a whole-database snapshot straight to `path`; returns bytes written."
  [client path]
  (with-open [out (BufferedOutputStream. (FileOutputStream. ^String path))]
    (reduce (fn [total ^bytes chunk] (.write out chunk) (+ total (alength chunk)))
            0 (snapshot client))))
