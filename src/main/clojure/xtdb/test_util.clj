(ns xtdb.test-util
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.client :as xtc]
            [xtdb.indexer :as idx]
            [xtdb.indexer.live-index :as li]
            [xtdb.log :as xtdb.log]
            [xtdb.logical-plan :as lp]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.query-ra :as ra]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [ch.qos.logback.classic Level Logger]
           clojure.lang.ExceptionInfo
           java.net.ServerSocket
           (java.io ByteArrayOutputStream FileOutputStream)
           (java.nio.channels Channels)
           (java.nio.file Files Path)
           java.nio.file.attribute.FileAttribute
           (java.time Duration Instant InstantSource Period)
           (java.util LinkedList TreeMap)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector FieldVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo Schema)
           (org.apache.arrow.vector.ipc ArrowFileWriter)
           org.slf4j.LoggerFactory
           (xtdb ICursor)
           (xtdb.api IXtdb TransactionKey)
           xtdb.api.log.Logs
           xtdb.api.query.IKeyFn
           xtdb.indexer.IIndexer
           xtdb.indexer.live_index.ILiveTable
           xtdb.util.RowCounter
           (xtdb.vector IVectorReader)))

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
(def ^:dynamic ^xtdb.api.IXtdb *node*)

(defn with-opts
  ([opts] (partial with-opts opts))
  ([opts f]
   (binding [*node-opts* (merge *node-opts* opts)]
     (f))))

(defn with-node [f]
  (util/with-open [node (xtn/start-node *node-opts*)]
    (binding [*node* node]
      (f))))

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

(defn latest-submitted-tx ^TransactionKey [node]
  (:latest-submitted-tx (xtp/status node)))

(defn then-await-tx
  (^TransactionKey [node]
   (then-await-tx (latest-submitted-tx node) node nil))

  (^TransactionKey [tx node]
   (then-await-tx tx node nil))

  (^TransactionKey [tx node ^Duration timeout]
   @(.awaitTxAsync ^IIndexer (util/component node :xtdb/indexer) tx timeout)))

(defn ->mock-clock
  (^java.time.InstantSource []
   (->mock-clock (iterate #(.plus ^Instant % (Period/ofDays 1))
                          (.toInstant #inst "2020-01-01"))))

  (^java.time.InstantSource [^Iterable insts]
   (let [it (.iterator insts)]
     (reify InstantSource
       (instant [_]
         (assert (.hasNext it) "out of insts!")
         (.next it))))))

(defn with-mock-clock [f]
  (with-opts {:log [:in-memory {:instant-src (->mock-clock)}]} f))

(defn finish-chunk! [node]
  (then-await-tx node)
  (li/finish-chunk! (component node :xtdb.indexer/live-index)))

(defn open-vec
  (^org.apache.arrow.vector.ValueVector [col-name-or-field vs]
   (vw/open-vec *allocator* col-name-or-field vs)))

(defn open-rel ^xtdb.vector.RelationReader [vecs]
  (vw/open-rel vecs))

(defn open-params ^xtdb.vector.RelationReader [params-map]
  (vw/open-params *allocator* params-map))

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
  ([query opts] (ra/query-ra query (assoc opts :allocator *allocator*))))

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

(defn ->local-node ^xtdb.api.IXtdb [{:keys [^Path node-dir ^String buffers-dir
                                            rows-per-chunk log-limit page-limit instant-src]
                                     :or {buffers-dir "objects"}}]
  (let [instant-src (or instant-src (->mock-clock))]
    (xtn/start-node {:log [:local {:path (.resolve node-dir "log"), :instant-src instant-src}]
                     :storage [:local {:path (.resolve node-dir buffers-dir)}]
                     :indexer (->> {:log-limit log-limit, :page-limit page-limit, :rows-per-chunk rows-per-chunk}
                                   (into {} (filter val)))})))

(defn ->local-submit-client ^xtdb.api.IXtdb [{:keys [^Path node-dir]}]
  (let [sys (-> {:xtdb/allocator {}
                 :xtdb/log (Logs/localLog (.resolve node-dir "log"))
                 :xtdb/default-tz #time/zone "UTC"}
                (ig/prep)
                (ig/init))]
    (reify IXtdb
      (submitTxAsync [_ tx-opts tx-ops]
        (xtdb.log/submit-tx& {:allocator (:xtdb/allocator sys)
                              :log (:xtdb/log sys)
                              :default-tz (:xtdb/default-tz sys)}
                             (vec tx-ops) tx-opts))
      (close [_]
        (ig/halt! sys)))))

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

(defn set-log-level! [ns level]
  (.setLevel ^Logger (LoggerFactory/getLogger (name ns))
             (when level
               (Level/valueOf (name level)))))

(defn get-log-level! [ns]
  (some->> (.getLevel ^Logger (LoggerFactory/getLogger (name ns)))
           (str)
           (.toLowerCase)
           (keyword)))

(defn with-log-levels* [ns-level-pairs f]
  (let [nses (mapv first ns-level-pairs)
        orig-levels (mapv get-log-level! nses)]
    (try
      (doseq [[ns l] ns-level-pairs] (set-log-level! ns l))
      (f)
      (finally
        (doseq [[ns ol] (mapv vector nses orig-levels)]
          (set-log-level! ns ol))))))

(defmacro with-log-levels [ns-level-pairs & body]
  `(with-log-levels* ~ns-level-pairs (fn [] ~@body)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defmacro with-log-level [ns level & body]
  `(with-log-levels {~ns ~level} ~@body))

(defn open-arrow-hash-trie-root ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator al, paths]
  (util/with-close-on-catch [meta-root (VectorSchemaRoot/create trie/meta-rel-schema al)]
    (let [meta-wtr (vw/root->writer meta-root)
          meta-wp (.writerPosition meta-wtr)
          nodes-wtr (.colWriter meta-wtr "nodes")
          nil-wtr (.legWriter nodes-wtr :nil)
          iid-branch-wtr (.legWriter nodes-wtr :branch-iid)
          iid-branch-el-wtr (.listElementWriter iid-branch-wtr)
          recency-branch-wtr (.legWriter nodes-wtr :branch-recency)
          recency-branch-el-wtr (.listElementWriter recency-branch-wtr)
          recency-wtr (.structKeyWriter recency-branch-el-wtr "recency")
          recency-idx-wtr (.structKeyWriter recency-branch-el-wtr "idx")

          data-wtr (.legWriter nodes-wtr :leaf)
          data-page-idx-wtr (.structKeyWriter data-wtr "data-page-idx")
          metadata-wtr (.structKeyWriter data-wtr "columns")]
      (letfn [(write-paths [paths]
                (cond
                  (nil? paths) (.writeNull nil-wtr)

                  (number? paths) (do
                                    (.startStruct data-wtr)
                                    (.writeInt data-page-idx-wtr paths)
                                    (.startList metadata-wtr)
                                    (.endList metadata-wtr)
                                    (.endStruct data-wtr))

                  (vector? paths) (let [!page-idxs (IntStream/builder)]
                                    (doseq [child paths]
                                      (.add !page-idxs (if child
                                                         (do
                                                           (write-paths child)
                                                           (dec (.getPosition meta-wp)))
                                                         -1)))
                                    (.startList iid-branch-wtr)
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
                                   (.put !page-idxs recency (dec (.getPosition meta-wp))))

                                 (.startList recency-branch-wtr)

                                 (doseq [[^long recency, ^long idx] !page-idxs]
                                   (.startStruct recency-branch-el-wtr)
                                   (.writeLong recency-wtr recency)
                                   (.writeInt recency-idx-wtr idx)
                                   (.endStruct recency-branch-el-wtr))

                                 (.endList recency-branch-wtr)))

                (.endRow meta-wtr))]
        (write-paths paths))

      (.syncRowCount meta-wtr))

    meta-root))

(defn write-arrow-data-file ^org.apache.arrow.vector.VectorSchemaRoot
  [^BufferAllocator al, page-idx->documents, ^Path data-file-path]
  (letfn [(normalize-doc [doc]
            (-> (dissoc doc :xt/system-from :xt/valid-from :xt/valid-to)
                (update-keys util/kw->normal-form-kw)))]
    (let [data-schema (->> page-idx->documents vals (apply concat) (filter #(= :put (first %)))
                           (map (comp vw/value->col-type normalize-doc second))
                           (apply types/merge-col-types))]
      (util/with-open [data-vsr (VectorSchemaRoot/create (trie/data-rel-schema data-schema) al)
                       data-wtr (vw/root->writer data-vsr)
                       os (FileOutputStream. (.toFile data-file-path))
                       write-ch (Channels/newChannel os)
                       aw (ArrowFileWriter. data-vsr nil write-ch)]
        (.start aw)
        (let [!last-iid (atom nil)
              iid-wtr (.colWriter data-wtr "xt$iid")
              system-from-wtr (.colWriter data-wtr "xt$system_from")
              valid-from-wtr (.colWriter data-wtr "xt$valid_from")
              valid-to-wtr (.colWriter data-wtr "xt$valid_to")
              op-wtr (.colWriter data-wtr "op")
              put-wtr (.legWriter op-wtr :put)
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

(defn ->live-data-rel [live-table]
  (trie/->LiveDataRel (vw/rel-wtr->rdr (li/live-rel live-table))))

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
