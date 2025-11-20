(ns xtdb.test-util
  (:require [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [clojure.test.check :as tc]
            [cognitect.anomalies :as-alias anom]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.indexer :as idx]
            [xtdb.log :as xt-log]
            [xtdb.logical-plan :as lp]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.protocols :as xtp]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang ExceptionInfo)
           java.net.ServerSocket
           java.nio.ByteBuffer
           (java.nio.file Files Path)
           java.nio.file.attribute.FileAttribute
           (java.time Duration Instant InstantSource LocalTime Period YearMonth ZoneId ZoneOffset)
           (java.time.temporal ChronoUnit)
           (java.util List)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector.types.pojo Field Schema)
           (org.testcontainers.containers GenericContainer)
           (xtdb ICursor PagesCursor)
           (xtdb.api TransactionKey)
           (xtdb.test.log RecordingLog$Factory)
           xtdb.api.query.IKeyFn
           (xtdb.arrow Relation RelationReader Vector VectorType)
           xtdb.database.Database$Catalog
           (xtdb.indexer LiveTable)
           (xtdb.log.proto TemporalMetadata TemporalMetadata$Builder)
           (xtdb.query IQuerySource PreparedQuery)
           xtdb.storage.BufferPool
           xtdb.types.ZonedDateTimeRange
           (xtdb.util RefCounter RowCounter TemporalBounds TemporalDimension)))

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic ^org.apache.arrow.memory.BufferAllocator *allocator*)

(defn with-allocator [f]
  (util/with-open [allocator (RootAllocator.)]
    (binding [*allocator* allocator]
      (f))))

(t/deftest test-memory-leak-doesnt-mask-original-error
  (t/is (thrown? ExceptionInfo
                 (with-allocator
                   (fn []
                     (.buffer *allocator* 10)
                     (throw (ex-info "boom!" {})))))))

(def ^:dynamic *node-opts* {})

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic ^xtdb.api.Xtdb *node*)

(defn with-opts
  ([opts] (partial with-opts opts))
  ([opts f]
   (binding [*node-opts* (merge *node-opts* opts)]
     (f))))

(defn with-node [f]
  (util/with-open [node (xtn/start-node *node-opts*)]
    (binding [*node* node]
      (f))))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn ->instants
  ([u] (->instants u 1))
  ([u len] (->instants u len #inst "2020-01-01"))
  ([u ^long len inst-like]
   (let [inst (time/->instant inst-like)
         zdt (.atZone inst (ZoneId/of "UTC"))
         year (.getYear zdt)
         month (.getValue (.getMonth zdt))]
     (letfn [(to-seq [^ChronoUnit unit]
               (->> (iterate #(.plus ^YearMonth % len unit) (YearMonth/of year month))
                    (map #(Instant/ofEpochSecond (.toEpochSecond (.atDay ^YearMonth % 1) LocalTime/MIDNIGHT ZoneOffset/UTC)))))]
       (case u
         :second (iterate #(.plus ^Instant % (Duration/ofSeconds len)) inst)
         :minute (iterate #(.plus ^Instant % (Duration/ofMinutes len)) inst)
         :hour (iterate #(.plus ^Instant % (Duration/ofHours len)) inst)
         :day (iterate #(.plus ^Instant % (Period/ofDays len)) inst)
         :month (to-seq ChronoUnit/MONTHS)
         :quarter (->> (iterate #(.plusMonths ^YearMonth % (* 3 len)) (YearMonth/of year month))
                       (map #(Instant/ofEpochSecond (.toEpochSecond (.atDay ^YearMonth % 1) LocalTime/MIDNIGHT ZoneOffset/UTC))))
         :year (to-seq ChronoUnit/YEARS))))))

(defn ->mock-clock
  (^java.time.InstantSource []
   (->mock-clock (iterate #(.plus ^Instant % (Period/ofDays 1))
                          (.toInstant #inst "2020-01-01"))))

  (^java.time.InstantSource [^Iterable insts]
   (let [it (.iterator insts)]
     (reify InstantSource
       (instant [_]
         (assert (.hasNext it) "out of insts!")
         (time/->instant (.next it)))))))

(defn with-mock-clock [f]
  (with-opts {:log [:in-memory {:instant-src (->mock-clock)}]} f))

(defn ->tstz-range ^xtdb.types.ZonedDateTimeRange [from to]
  (ZonedDateTimeRange. (time/->zdt from) (some-> to time/->zdt)))

(defn finish-block! [node]
  (.finishBlock (.getLogProcessor (db/primary-db node))))

(defn flush-block!
  ([node] (flush-block! node #xt/duration "PT5S"))
  ([node timeout]
   (let [db-cat (db/<-node node)]
     (doseq [db-name (.getDatabaseNames db-cat)]
       (xt-log/send-flush-block-msg! (.databaseOrNull db-cat db-name))))

   (xt-log/sync-node node timeout)))

(defn open-vec
  (^xtdb.arrow.Vector [^Field field]
   (Vector/open *allocator* field))

  (^xtdb.arrow.Vector [col-name-or-field ^List rows]
   (cond
     (string? col-name-or-field) (Vector/fromList *allocator* ^String col-name-or-field rows)
     (instance? Field col-name-or-field) (Vector/fromList *allocator* ^Field col-name-or-field rows)
     :else (throw (err/incorrect ::invalid-vec {:col-name-or-field col-name-or-field}))))

  (^xtdb.arrow.Vector [col-name vec-type rows]
   (Vector/fromList *allocator* col-name vec-type rows)))

(defn open-rel
  (^xtdb.arrow.Relation [] (Relation. *allocator*))

  (^xtdb.arrow.RelationReader [rows-or-cols]
   (cond
     (and (map? rows-or-cols) (every? sequential? (vals rows-or-cols)))
     (Relation/openFromCols *allocator* rows-or-cols)

     (and (sequential? rows-or-cols) (every? map? rows-or-cols))
     (Relation/openFromRows *allocator* rows-or-cols)

     :else (throw (err/incorrect ::invalid-rel {:rows-or-cols rows-or-cols})))))

(defn open-args ^xtdb.arrow.RelationReader [args]
  (vw/open-args *allocator* args))

(defn ->cursor
  (^xtdb.ICursor [pages] (->cursor *allocator* pages))
  (^xtdb.ICursor [^BufferAllocator allocator, pages] (->cursor allocator nil pages))
  (^xtdb.ICursor [^BufferAllocator allocator, schema pages] (PagesCursor. allocator schema pages)))

(defmethod lp/ra-expr ::pages [_]
  (s/cat :op #{::pages}
         :vec-types (s/? (s/map-of simple-symbol? #(instance? VectorType %)))
         :pages vector?))

(defn rows->fields [rows]
  (->> (for [col-name (into #{} (mapcat keys) rows)]
         [(symbol col-name) (-> rows
                                (->> (into #{} (map (fn [row]
                                                      (types/value->vec-type (get row col-name)))))
                                     (apply types/merge-types))
                                (types/->field (str (symbol col-name))))])
       (into {})))

(defmethod lp/emit-expr ::pages [{:keys [vec-types pages stats]} _args]
  (let [fields (or (some->> vec-types
                            (into {} (map (fn [[col-name vec-type]]
                                            [col-name (types/vec-type->field vec-type col-name)]))))
                   (rows->fields (into [] cat pages)))
        ^Schema schema (Schema. (for [[col-name field] fields]
                                  (types/field-with-name field (str col-name))))]
    {:op :pages
     :children []
     :fields fields
     :stats stats
     :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]}]
                 (cond-> (->cursor allocator schema pages)
                   (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))}))


(defmethod lp/ra-expr :prn [_]
  (s/cat :op #{:prn}
         :relation ::lp/ra-expression))

(defmethod lp/emit-expr :prn [{:keys [relation]} _args]
  (lp/unary-expr (lp/emit-expr relation _args)
    (fn [{inner-fields :fields, :as inner-rel}]
      {:op :prn
       :stats (:stats inner-rel)
       :children [inner-rel]
       :fields inner-fields
       :->cursor (fn [{:keys [explain-analyze? tracer query-span]}, ^ICursor in-cursor]
                   (cond-> (reify ICursor
                             (getCursorType [_] "prn")
                             (getChildCursors [_] [in-cursor])

                             (tryAdvance [_ c]
                               (.tryAdvance in-cursor
                                            (fn [^RelationReader rel]
                                              (println (.getCursorType in-cursor) ":")
                                              (clojure.pprint/pprint (util/->clj (.getAsMaps rel)))
                                              (.accept c rel)))))
                     (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))})))

(defn <-cursor
  ([^ICursor cursor] (<-cursor cursor #xt/key-fn :kebab-case-keyword))
  ([^ICursor cursor ^IKeyFn key-fn] (.consume cursor key-fn)))

(defn query-ra
  ([query] (query-ra query {}))
  ([query {:keys [node args preserve-pages? with-types? key-fn] :as query-opts
           :or {key-fn (serde/read-key-fn :kebab-case-keyword)}}]
   (let [allocator (:allocator node *allocator*)
         query-opts (-> query-opts
                        (update :default-db (fnil identity "xtdb"))
                        (cond-> node (-> (update :await-token (fnil identity (xtp/await-token node)))
                                         (doto (-> :await-token (->> (xt-log/await-node node)))))))

         [^IQuerySource q-src close-q-src?] (if node
                                              [(util/component node ::q/query-source) false]
                                              [(q/->query-source {:allocator allocator
                                                                  :ref-ctr (RefCounter.)})
                                               true])]

     (try
       (let [^PreparedQuery pq (.prepareQuery q-src query (or (db/<-node node) Database$Catalog/EMPTY) query-opts)]

         (util/with-open [^RelationReader args-rel (if args
                                                     (vw/open-args allocator args)
                                                     vw/empty-args)
                          res (.openQuery pq (-> (select-keys query-opts [:snapshot-token :snapshot-time :current-time
                                                                          :await-token :table-args :default-tz])
                                                 (assoc :args args-rel, :close-args? false)))]
           (let [rows (-> (<-cursor res (serde/read-key-fn key-fn))
                          (cond->> (not preserve-pages?) (into [] cat)))]
             (if with-types?
               {:res rows,
                :types (->> (.getResultFields res)
                            (into {} (map (juxt #(symbol (.getName ^Field %))
                                                VectorType/fromField))))}
               rows))))
       (finally
         (when close-q-src?
           (util/close q-src)))))))

(t/deftest round-trip-cursor
  (with-allocator
    (fn []
      (let [pages [[{:name "foo", :age 20}
                    {:name "bar", :age 25}]
                   [{:name "baz", :age 30}]]]
        (with-open [cursor (->cursor pages)]
          (t/is (= pages (<-cursor cursor))))))))

(defn ->local-node ^xtdb.api.Xtdb [{:keys [^Path node-dir ^String buffers-dir
                                           rows-per-block log-limit page-limit instant-src
                                           compactor-threads healthz-port gc? blocks-to-keep garbage-lifetime
                                           instant-source-for-non-tx-msgs? storage-epoch default-tz tracer]
                                    :or {buffers-dir "objects"
                                         healthz-port 8080
                                         instant-source-for-non-tx-msgs? false
                                         storage-epoch 0
                                         default-tz (ZoneId/of "Europe/London")}}]
  (let [instant-src (or instant-src (->mock-clock))
        healthz-port (if (util/port-free? healthz-port) healthz-port (util/free-port))]
    (xtn/start-node (cond-> {:healthz {:port healthz-port
                                       :host "*"}
                             :log [:local {:path (.resolve node-dir "log"), :instant-src instant-src
                                           :instant-source-for-non-tx-msgs? instant-source-for-non-tx-msgs?}]
                             :storage [:local {:path (.resolve node-dir buffers-dir)
                                               :epoch storage-epoch}]
                             :indexer (->> {:log-limit log-limit, :page-limit page-limit, :rows-per-block rows-per-block}
                                           (into {} (filter val)))
                             :compactor (->> {:threads compactor-threads}
                                             (into {} (filter val)))
                             :default-tz default-tz}
                      (not (nil? gc?)) (assoc :garbage-collector
                                              (cond-> {:enabled? gc?}
                                                blocks-to-keep (assoc :blocks-to-keep blocks-to-keep)
                                                garbage-lifetime (assoc :garbage-lifetime garbage-lifetime)))
                      tracer (assoc :tracer tracer)))))

(defn with-tmp-dir* [prefix f]
  (let [dir (Files/createTempDirectory prefix (make-array FileAttribute 0))]
    (try
      (f dir)
      (finally
        (util/delete-dir dir)))))

(defmacro with-tmp-dirs
  "Usage:
    (with-tmp-dirs #{log-dir objects-dir}
      ...)"
  [[dir-binding & more-bindings] & body]
  (if dir-binding
    `(with-tmp-dir* ~(name dir-binding)
       (fn [~(vary-meta dir-binding assoc :tag 'java.nio.file.Path)]
         (with-tmp-dirs #{~@more-bindings}
           ~@body)))
    `(do ~@body)))

(defn ->temporal-bounds
  ([min max] (->temporal-bounds min max min))
  ([vf-min vt-max sf-min] (->temporal-bounds vf-min vt-max sf-min Long/MAX_VALUE))
  ([vf-min vt-max sf-min st-max] (TemporalBounds. (TemporalDimension. vf-min vt-max) (TemporalDimension. sf-min st-max))))

(defn ->temporal-metadata
  ([min max] (->temporal-metadata min max min))
  ([vf-min vt-max sf-min] (->temporal-metadata vf-min vt-max sf-min sf-min))
  ([vf-min vt-max sf-min sf-max]
   (let [^TemporalMetadata$Builder builder (TemporalMetadata/newBuilder)]
     (.setMinValidFrom builder vf-min)
     (.setMaxValidFrom builder vf-min)
     (.setMinValidTo builder vt-max)
     (.setMaxValidTo builder vt-max)
     (.setMinSystemFrom builder sf-min)
     (.setMaxSystemFrom builder sf-max)
     (.build builder))))

(defn open-live-table ^xtdb.indexer.LiveTable [table]
  (LiveTable. *allocator* BufferPool/UNUSED table (RowCounter.)))

(defn index-tx! [^LiveTable live-table, ^TransactionKey tx-key, docs]
  (let [system-time (.getSystemTime tx-key)
        live-table-tx (.startTx live-table tx-key true)]
    (try
      (let [doc-wtr (.getDocWriter live-table-tx)]
        (doseq [{eid :xt/id, :as doc} docs
                :let [{:keys [:xt/valid-from :xt/valid-to],
                       :or {valid-from system-time, valid-to (time/micros->instant Long/MAX_VALUE)}} (meta doc)]]
          (.logPut live-table-tx (ByteBuffer/wrap (util/->iid eid))
                   (time/instant->micros valid-from) (time/instant->micros valid-to)
                   (fn [] (.writeObject doc-wtr doc)))))
      (catch Throwable t
        (.abort live-table-tx)
        (throw t)))

    (.commit live-table-tx)))

(defn bytes->path [^bytes bs]
  (mapcat (fn [b]
            [(bit-and (bit-shift-right b 6) 3)
             (bit-and (bit-shift-right b 4) 3)
             (bit-and (bit-shift-right b 2) 3)
             (bit-and b 3)])
          bs))

(defn q-sql
  "Like xtdb.api/q, but also returns the result type."
  ([node query] (q-sql node query {}))
  ([node query opts]
   (let [^PreparedQuery prepared-q (xtp/prepare-sql node query (merge {:default-db "xtdb"} opts))]
     {:res (xt/q node query opts)
      :res-type (mapv (juxt #(.getName ^Field %) types/field->col-type) (.getColumnFields prepared-q []))})))

(defn with-container [^GenericContainer c, f]
  (if (.getContainerId c)
    (f c)
    (try
      (.start c)
      (f c)
      (finally
        (.stop c)))))

(def property-test-iterations
  (Integer/parseInt (System/getProperty "xtdb.property-test-iterations" "100")))

(defn run-property-test
  "Takes opts map as first argument which is passed to quick-check (e.g. {:seed 42, :num-tests 100})"
  ([property]
   (run-property-test {} property))
  ([opts property]
   (let [opts (merge {:num-tests property-test-iterations} opts)
         result (tc/quick-check (:num-tests opts) property (dissoc opts :num-tests))]
     (t/is (:pass? result) (with-out-str (pp/pprint result))))))

(defn remove-nils [m]
  (into {} (remove (comp nil? val) m)))

(defn read-files-from-bp-path [node ^Path path]
  (->> (.listAllObjects (.getBufferPool (db/primary-db node)) (util/->path path))
       (mapv (comp str #(.getFileName ^Path %) :key os/<-StoredObject))))

(defmethod xt-log/->log-factory ::recording [_ {:keys [instant-src]}]
  (cond-> (RecordingLog$Factory.)
    instant-src (.instantSource instant-src)))
