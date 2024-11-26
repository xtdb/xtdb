(ns xtdb.test-util
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.optional :as jdbc.optional]
            [next.jdbc.prepare :as jdbc.prep]
            [next.jdbc.result-set :as jdbc.rs]
            [xtdb.client :as xtc]
            [xtdb.indexer :as idx]
            [xtdb.indexer.live-index :as li]
            [xtdb.logical-plan :as lp]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang ExceptionInfo)
           (java.io FileOutputStream)
           java.net.ServerSocket
           (java.nio.channels Channels)
           (java.nio.file Files Path)
           java.nio.file.attribute.FileAttribute
           (java.sql PreparedStatement Types)
           (java.time Instant InstantSource LocalTime Period YearMonth ZoneId ZoneOffset)
           (java.time.temporal ChronoUnit)
           (java.util LinkedList TreeMap)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector FieldVector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc ArrowFileWriter)
           (org.apache.arrow.vector.types.pojo Field Schema)
           (xtdb ICursor)
           (xtdb.api TransactionKey)
           xtdb.api.query.IKeyFn
           xtdb.arrow.Relation
           xtdb.indexer.live_index.ILiveTable
           (xtdb.query BoundQuery IQuerySource PreparedQuery)
           xtdb.types.ZonedDateTimeRange
           (xtdb.util RefCounter RowCounter TemporalBounds TemporalDimension)
           (xtdb.vector IVectorReader RelationReader)
           (xtdb.watermark IWatermarkSource Watermark)))

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

(def ^:dynamic *node-opts* {:server {:port 0}})

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic ^xtdb.api.Xtdb *node*)

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic ^java.sql.Connection *conn*)

(defn with-opts
  ([opts] (partial with-opts opts))
  ([opts f]
   (binding [*node-opts* (merge *node-opts* opts)]
     (f))))

(defn node->server [node]
  (-> node :system :xtdb.pgwire/server))

(defn with-node [f]
  (util/with-open [node (xtn/start-node *node-opts*)
                   conn (jdbc/get-connection node)]
    (binding [*node* node, *conn* conn]
      (f))))

(extend-protocol jdbc.prep/SettableParameter
  java.util.Date
  (set-parameter [v ^PreparedStatement ps ^long i]
    (.setObject ps i (-> (.toInstant v) (.atZone #time/zone "Z") (.toLocalDateTime)) Types/TIMESTAMP)))

(def jdbc-qopts
  {:builder-fn
   (jdbc.rs/as-maps-adapter
     (fn [rs opts]
       (jdbc.optional/as-unqualified-modified-maps rs (-> opts (assoc :label-fn xt-jdbc/label-fn))))
     xt-jdbc/col-reader)})

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic *sys*)

(defn with-system [sys-opts f]
  (let [sys (-> sys-opts
                (doto ig/load-namespaces)
                ig/prep
                ig/init)]
    (try
      (binding [*sys* sys]
        (f))
      (finally
        (ig/halt! sys)))))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(def ^:dynamic *http-port* nil)

(defn with-http-client-node [f]
  (binding [*http-port* (free-port)]
    (util/with-open [_ (xtn/start-node (-> *node-opts*
                                           (assoc-in [:http-server :port] *http-port*)))]
      (binding [*node* (xtc/start-client (str "http://localhost:" *http-port*))]
        (f)))))

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic *node-type*)

(defn with-each-api-implementation [api-implementations]
  (fn [f]
    (doseq [[node-type run-tests] api-implementations]
      (binding [*node-type* node-type]
        (t/testing (str node-type)
          (run-tests f))))))

(defn component
  ([k] (component *node* k))
  ([node k] (util/component node k)))

(defn latest-completed-tx ^TransactionKey [node]
  (:latest-completed-tx (xtp/status node)))

(defn latest-submitted-tx-id ^TransactionKey [node]
  (xtp/latest-submitted-tx-id node))

;; TODO inline this now that we have `idx/await-tx`
(defn then-await-tx
  (^TransactionKey [node]
   (then-await-tx (latest-submitted-tx-id node) node nil))

  (^TransactionKey [tx-id node]
   (then-await-tx tx-id node nil))

  (^TransactionKey [tx-id node timeout]
   (idx/await-tx tx-id node timeout)))

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
         :second (iterate #(.plusMillis ^Instant % (* 1000 len)) inst)
         :minute (iterate #(.plusMillis ^Instant % (* 1000 60 len)) inst)
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

(defn finish-chunk! [node]
  (then-await-tx node)
  (li/finish-chunk! (component node :xtdb.indexer/live-index)))

(defn open-vec
  (^org.apache.arrow.vector.ValueVector [col-name-or-field vs]
   (vw/open-vec *allocator* col-name-or-field vs)))

(defn open-rel ^xtdb.vector.RelationReader [vecs]
  (vw/open-rel vecs))

(defn open-args ^xtdb.vector.RelationReader [args]
  (vw/open-args *allocator* args))

(defn populate-root ^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root rows]
  (.clear root)

  (let [field-vecs (.getFieldVectors root)
        row-count (count rows)]
    (doseq [^FieldVector field-vec field-vecs]
      (vw/write-vec! field-vec (map (keyword (.getName (.getField field-vec))) rows)))

    (.setRowCount root row-count)
    root))

(defn ->cursor
  (^xtdb.ICursor [^Schema schema, blocks] (->cursor *allocator* schema blocks))

  (^xtdb.ICursor [^BufferAllocator allocator ^Schema schema, blocks]
   (let [blocks (LinkedList. blocks)
         root (VectorSchemaRoot/create schema allocator)]
     (reify ICursor
       (tryAdvance [_ c]
         (if-let [block (some-> (.poll blocks) vec)]
           (do
             (populate-root root block)
             (.accept c (vr/<-root root))
             true)
           false))

       (close [_]
         (.close root))))))

(defmethod lp/ra-expr ::blocks [_]
  (s/cat :op #{::blocks}
         :col-types (s/? (s/map-of simple-symbol? some?))
         :blocks vector?))

(defmethod lp/emit-expr ::blocks [{:keys [col-types blocks stats]} _args]
  (let [fields (or (some-> col-types (update-vals types/col-type->field))
                   (vw/rows->fields (into [] cat blocks)))
        ^Schema schema (Schema. (for [[col-name field] fields]
                                  (types/field-with-name field (str col-name))))]
    {:fields fields
     :stats stats
     :->cursor (fn [{:keys [allocator]}]
                 (->cursor allocator schema blocks))}))

(defn <-reader
  ([^IVectorReader col] (<-reader col #xt/key-fn :kebab-case-keyword))
  ([^IVectorReader col ^IKeyFn key-fn]
   (mapv (fn [idx]
           (.getObject col idx key-fn))
         (range (.valueCount col)))))

(defn <-cursor
  ([^ICursor cursor] (<-cursor cursor #xt/key-fn :kebab-case-keyword))
  ([^ICursor cursor ^IKeyFn key-fn]
   (let [!res (volatile! (transient []))]
     (.forEachRemaining cursor
                        (reify Consumer
                          (accept [_ rel]
                            (vswap! !res conj! (vr/rel->rows rel key-fn)))))
     (persistent! @!res))))

(defn query-ra
  ([query] (query-ra query {}))
  ([query {:keys [allocator node args preserve-blocks? with-col-types? key-fn] :as query-opts
           :or {key-fn (serde/read-key-fn :kebab-case-keyword)
                allocator *allocator*}}]
   (let [indexer (util/component node :xtdb/indexer)
         query-opts (-> query-opts
                        (assoc :allocator allocator)
                        (cond-> node (-> (update :after-tx-id (fnil identity (xtp/latest-submitted-tx-id node)))
                                         (doto (-> :after-tx-id (idx/await-tx node))))))

         ^PreparedQuery pq (if node
                             (let [^IQuerySource q-src (util/component node ::q/query-source)]
                               (.prepareRaQuery q-src query indexer query-opts))
                             (q/prepare-ra query {:ref-ctr (RefCounter.)
                                                  :wm-src (reify IWatermarkSource
                                                            (openWatermark [_]
                                                              (Watermark. nil nil {})))}))]
     (util/with-open [^RelationReader args-rel (if args
                                                 (vw/open-args allocator args)
                                                 vw/empty-args)
                      bq (.bind pq (-> (select-keys query-opts [:snapshot-time :current-time :after-tx-id :table-args :default-tz])
                                       (assoc :args args-rel, :close-args? false)))
                      res (.openCursor bq)]
       (let [rows (-> (<-cursor res (serde/read-key-fn key-fn))
                      (cond->> (not preserve-blocks?) (into [] cat)))]
         (if with-col-types?
           {:res rows, :col-types (->> (.columnFields bq)
                                       (into {} (map (juxt #(symbol (.getName ^Field %)) types/field->col-type))))}
           rows))))))

(t/deftest round-trip-cursor
  (with-allocator
    (fn []
      (let [blocks [[{:name "foo", :age 20}
                     {:name "bar", :age 25}]
                    [{:name "baz", :age 30}]]]
        (with-open [cursor (->cursor (Schema. [(types/col-type->field "name" :utf8)
                                               (types/col-type->field "age" :i64)])
                                     blocks)]

          (t/is (= blocks (<-cursor cursor))))))))

(defn ->local-node ^xtdb.api.Xtdb [{:keys [^Path node-dir ^String buffers-dir
                                           rows-per-chunk log-limit page-limit instant-src
                                           compactor?]
                                    :or {compactor? true buffers-dir "objects"}}]
  (let [instant-src (or instant-src (->mock-clock))]
    (xtn/start-node {:server {:port 0}
                     :log [:local {:path (.resolve node-dir "log"), :instant-src instant-src}]
                     :storage [:local {:path (.resolve node-dir buffers-dir)}]
                     :indexer (->> {:log-limit log-limit, :page-limit page-limit, :rows-per-chunk rows-per-chunk}
                                   (into {} (filter val)))
                     :compactor {:enabled? compactor?}})))

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

(defn ->min-max-page-bounds
  ([min max] (->min-max-page-bounds min max min))
  ([vt-min vt-max st-min] (->min-max-page-bounds vt-min vt-max st-min Long/MAX_VALUE))
  ([vt-min vt-max st-min st-max]
   (TemporalBounds. (TemporalDimension. vt-min vt-max) (TemporalDimension. st-min st-max))))

(defn ->page-bounds-fn [page-idx-pred->bounds]
  (let [page-idx-pred->bounds (update-vals page-idx-pred->bounds #(apply ->min-max-page-bounds %))]
    (fn page-bounds-fn [page-idx]
      (if-let [bounds (reduce-kv (fn [_ page-idx-pred bounds]
                                   (when (page-idx-pred page-idx)
                                     (reduced bounds)))
                                 nil
                                 page-idx-pred->bounds)]
        bounds
        (throw (IllegalStateException. (str "No bounds found for page " page-idx "!")))))))

(defn open-arrow-hash-trie-rel ^xtdb.arrow.Relation [^BufferAllocator al, paths]
  (util/with-close-on-catch [meta-rel (Relation. al trie/meta-rel-schema)]
    (let [nodes-wtr (.get meta-rel "nodes")
          nil-wtr (.legWriter nodes-wtr "nil")
          iid-branch-wtr (.legWriter nodes-wtr "branch-iid")
          iid-branch-el-wtr (.elementWriter iid-branch-wtr)
          recency-branch-wtr (.legWriter nodes-wtr "branch-recency")
          recency-branch-el-wtr (.elementWriter recency-branch-wtr)
          recency-wtr (.keyWriter recency-branch-el-wtr "recency")
          recency-idx-wtr (.keyWriter recency-branch-el-wtr "idx")

          data-wtr (.legWriter nodes-wtr "leaf")
          data-page-idx-wtr (.keyWriter data-wtr "data-page-idx")
          metadata-wtr (.keyWriter data-wtr "columns")]
      (letfn [(write-paths [paths]
                (cond
                  (nil? paths) (.writeNull nil-wtr)

                  (number? paths) (do
                                    (.writeInt data-page-idx-wtr paths)
                                    (.endList metadata-wtr)
                                    (.endStruct data-wtr))

                  (vector? paths) (let [!page-idxs (IntStream/builder)]
                                    (doseq [child paths]
                                      (.add !page-idxs (if child
                                                         (do
                                                           (write-paths child)
                                                           (dec (.getRowCount meta-rel)))
                                                         -1)))
                                    (.forEach (.build !page-idxs)
                                              (reify IntConsumer
                                                (accept [_ idx]
                                                  (if (= idx -1)
                                                    (.writeNull iid-branch-el-wtr)
                                                    (.writeInt iid-branch-el-wtr idx)))))
                                    (.endList iid-branch-wtr))

                  (map? paths) (let [!page-idxs (TreeMap.)]
                                 (doseq [[recency child] paths]
                                   (write-paths child)
                                   (.put !page-idxs recency (dec (.getRowCount meta-rel))))

                                 (doseq [[^long recency, ^long idx] !page-idxs]
                                   (.writeLong recency-wtr recency)
                                   (.writeInt recency-idx-wtr idx)
                                   (.endStruct recency-branch-el-wtr))

                                 (.endList recency-branch-wtr)))

                (.endRow meta-rel))]
        (write-paths paths)))

    meta-rel))

(defn verify-hash-tries+page-bounds [paths page-bounds]
  (letfn [(get-bounds [page-idx]
            (or (reduce-kv (fn [_ page-idx-pred bounds] (when (page-idx-pred page-idx) (reduced bounds))) nil page-bounds)
                (throw (IllegalStateException. "Missing page bounds!"))))
          (get-receny-paths [paths]
            (cond (nil? paths) nil
                  (number? paths) (list (list paths))
                  (vector? paths) (->> (mapcat get-receny-paths paths)
                                       (filter identity))
                  (map? paths) (mapcat (fn [[k v]]
                                         (->> (get-receny-paths v)
                                              (filter identity)
                                              (map #(cons k %)))) paths)))]
    (let [recency-paths (get-receny-paths paths)]
      (doseq [recency-path recency-paths
              :let [recencies (butlast recency-path)
                    page-idx (last recency-path)
                    [_min-vt max-vt _min-st max-st] (get-bounds page-idx)
                    max-st (or max-st Long/MAX_VALUE)]]
        (assert (apply >= recencies))
        (doseq [recency recencies]
          (assert (<= (min max-vt max-st) recency)))))))

(defn write-arrow-data-file ^org.apache.arrow.vector.VectorSchemaRoot
  [^BufferAllocator al, page-idx->documents, ^Path data-file-path]
  (letfn [(normalize-doc [doc]
            (-> (dissoc doc :xt/system-from :xt/valid-from :xt/valid-to)
                (update-keys util/kw->normal-form-kw)))]
    (let [data-schema (-> page-idx->documents
                          (->> vals (apply concat) (filter #(= :put (first %)))
                               (map (comp types/col-type->field vw/value->col-type normalize-doc second))
                               (apply types/merge-fields))
                          (types/field-with-name "put"))]
      (util/with-open [data-vsr (VectorSchemaRoot/create (trie/data-rel-schema data-schema) al)
                       data-wtr (vw/root->writer data-vsr)
                       os (FileOutputStream. (.toFile data-file-path))
                       write-ch (Channels/newChannel os)
                       aw (ArrowFileWriter. data-vsr nil write-ch)]
        (.start aw)
        (let [!last-iid (atom nil)
              iid-wtr (.colWriter data-wtr "_iid")
              system-from-wtr (.colWriter data-wtr "_system_from")
              valid-from-wtr (.colWriter data-wtr "_valid_from")
              valid-to-wtr (.colWriter data-wtr "_valid_to")
              op-wtr (.colWriter data-wtr "op")
              put-wtr (.legWriter op-wtr "put")
              max-page-id (-> (keys page-idx->documents) sort last)]
          (doseq [i (range (inc max-page-id))]
            (doseq [[op doc] (get page-idx->documents i)]
              (case op
                :put (let [iid-bytes (trie/->iid (:xt/id doc))]
                       (when (and @!last-iid (> (util/compare-nio-buffers-unsigned @!last-iid iid-bytes) 0))
                         (log/error "IID's not in required order!" (:xt/id doc)))
                       (.startRow data-wtr)
                       (.writeObject iid-wtr iid-bytes)
                       (.writeLong system-from-wtr (or (:xt/system-from doc) 0))
                       (.writeLong valid-from-wtr (or (:xt/valid-from doc) 0))
                       (.writeLong valid-to-wtr (or (:xt/valid-to doc) Long/MAX_VALUE))
                       (.writeObject put-wtr (normalize-doc doc))
                       (.endRow data-wtr)
                       (reset! !last-iid iid-bytes))
                (:delete :erase) (throw (UnsupportedOperationException.))))
            (.syncRowCount data-wtr)
            (.writeBatch aw)
            (.clear data-wtr)
            (.clear data-vsr))
          (.end aw)))
      data-file-path)))

(defn open-live-table [table-name]
  (li/->live-table *allocator* nil (RowCounter. 0) table-name))

(defn index-tx! [^ILiveTable live-table, ^TransactionKey tx-key, docs]
  (let [system-time (.getSystemTime tx-key)
        live-table-tx (.startTx live-table tx-key true)]
    (try
      (let [doc-wtr (.docWriter live-table-tx)]
        (doseq [{eid :xt/id, :as doc} docs
                :let [{:keys [:xt/valid-from :xt/valid-to],
                       :or {valid-from system-time, valid-to (time/micros->instant Long/MAX_VALUE)}} (meta doc)]]
          (.logPut live-table-tx (trie/->iid eid)
                   (time/instant->micros valid-from) (time/instant->micros valid-to)
                   (fn [] (.writeObject doc-wtr doc)))))
      (catch Throwable t
        (.abort live-table-tx)
        (throw t)))

    (.commit live-table-tx)))

(defn byte-buffer->path [^java.nio.ByteBuffer bb]
  (mapcat (fn [b]
            [(bit-and (bit-shift-right b 6) 3)
             (bit-and (bit-shift-right b 4) 3)
             (bit-and (bit-shift-right b 2) 3)
             (bit-and b 3)])
          (.array bb)))

(defn uuid-seq [n]
  (letfn [(new-uuid [n]
            (java.util.UUID. (Long/reverse n) 0))]
    (map new-uuid (range n))))

(defn bad-uuid-seq
  ([n] (bad-uuid-seq 0 n))
  ([start end]
   (letfn [(new-uuid [n]
             (java.util.UUID. 0 n))]
     (map new-uuid (range start end)))))

(defn vec->vals
  ([^IVectorReader rdr] (vec->vals rdr #xt/key-fn :kebab-case-keyword))
  ([^IVectorReader rdr ^IKeyFn key-fn]
   (->> (for [i (range (.valueCount rdr))]
          (.getObject rdr i key-fn))
        (into []))))

(defn get-extension [^Path path]
  (let [name (str (.getFileName path))]
    (when-let [idx (str/last-index-of name ".")]
      (subs name (inc idx)))))
