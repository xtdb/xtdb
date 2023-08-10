(ns xtdb.test-util
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api.protocols :as api]
            [xtdb.client :as client]
            [xtdb.indexer :as idx]
            xtdb.ingester
            [xtdb.logical-plan :as lp]
            [xtdb.node :as node]
            xtdb.object-store
            [xtdb.operator :as op]
            xtdb.operator.scan
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [ch.qos.logback.classic Level Logger]
           clojure.lang.ExceptionInfo
           java.net.ServerSocket
           (java.nio.file Files Path)
           java.nio.file.attribute.FileAttribute
           (java.time Duration Instant Period)
           (java.util LinkedList)
           java.util.function.Consumer
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector FieldVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo Schema)
           org.slf4j.LoggerFactory
           (xtdb ICursor InstantSource)
           xtdb.indexer.IIndexer
           (xtdb.operator IRaQuerySource PreparedQuery)
           (xtdb.vector IVectorReader RelationReader)))

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
(def ^:dynamic *node*)

(defn with-opts
  ([opts] (partial with-opts opts))
  ([opts f]
   (binding [*node-opts* (merge *node-opts* opts)]
     (f))))

(defn with-node [f]
  (util/with-open [node (node/start-node *node-opts*)]
    (binding [*node* node]
      (f))))

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic *sys*)

(defn with-system [sys-opts]
  (fn [f]
    (let [sys (-> sys-opts
                  (doto ig/load-namespaces)
                  ig/prep
                  ig/init)]
      (try
        (binding [*sys* sys]
          (f))
        (finally
          (ig/halt! sys))))))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn with-http-client-node [f]
  (let [port (free-port)]
    (util/with-open [_ (node/start-node (-> *node-opts*
                                            (assoc-in [:xtdb/server :port] port)))]
      (binding [*node* (client/start-client (str "http://localhost:" port))]
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

(defn then-await-tx
  (^xtdb.api.protocols.TransactionInstant [tx node]
   (then-await-tx tx node nil))

  (^xtdb.api.protocols.TransactionInstant [tx node ^Duration timeout]
   @(.awaitTxAsync ^IIndexer (util/component node :xtdb/indexer) tx timeout)))

(defn latest-completed-tx ^xtdb.api.protocols.TransactionInstant [node]
  (:latest-completed-tx (api/status node)))

(defn ->mock-clock
  (^xtdb.InstantSource []
   (->mock-clock (iterate #(.plus ^Instant % (Period/ofDays 1))
                          (.toInstant #inst "2020-01-01"))))

  (^xtdb.InstantSource [^Iterable insts] (InstantSource/mock insts)))

(defn with-mock-clock [f]
  (with-opts {:xtdb.log/memory-log {:instant-src (->mock-clock)}} f))

(defn finish-chunk! [node]
  (idx/finish-chunk! (component node :xtdb/indexer)))

(defn open-vec
  (^org.apache.arrow.vector.ValueVector [col-name vs]
   (vw/open-vec *allocator* col-name vs))

  (^org.apache.arrow.vector.ValueVector [col-name col-type vs]
   (vw/open-vec *allocator* col-name col-type vs)))

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
  (let [col-types (or col-types
                      (vw/rows->col-types (into [] cat blocks)))
        ^Schema schema (Schema. (for [[col-name col-type] col-types]
                                  (types/col-type->field col-name col-type)))]
    {:col-types col-types
     :stats stats
     :->cursor (fn [{:keys [allocator]}]
                 (->cursor allocator schema blocks))}))

(defn <-reader [^IVectorReader col]
  (mapv (fn [idx]
          (.getObject col idx))
        (range (.valueCount col))))

(defn <-cursor [^ICursor cursor]
  (let [!res (volatile! (transient []))]
    (.forEachRemaining cursor
                       (reify Consumer
                         (accept [_ rel]
                           (vswap! !res conj! (vr/rel->rows rel)))))
    (persistent! @!res)))

(defn query-ra
  ([query] (query-ra query {}))
  ([query {:keys [node params preserve-blocks? with-col-types?] :as query-opts}]
   (let [^IIndexer indexer (util/component node :xtdb/indexer)
         query-opts (cond-> query-opts
                      node (-> (update :basis api/after-latest-submitted-tx node)
                               (doto (-> :basis :after-tx (then-await-tx node)))))]

     (with-open [^RelationReader
                 params-rel (if params
                              (vw/open-params *allocator* params)
                              vw/empty-params)]
       (let [^PreparedQuery pq (if node
                                 (let [^IRaQuerySource ra-src (util/component node ::op/ra-query-source)]
                                   (.prepareRaQuery ra-src query))
                                 (op/prepare-ra query))
             bq (.bind pq indexer
                       (-> (select-keys query-opts [:basis :table-args :default-tz :default-all-valid-time?])
                           (assoc :params params-rel)))]
         (with-open [res (.openCursor bq)]
           (let [rows (-> (<-cursor res)
                          (cond->> (not preserve-blocks?) (into [] cat)))]
             (if with-col-types?
               {:res rows, :col-types (.columnTypes bq)}
               rows))))))))

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

(defn ->local-node ^java.lang.AutoCloseable [{:keys [^Path node-dir ^String buffers-dir
                                                     rows-per-block rows-per-chunk instant-src]
                                              :or {buffers-dir "buffers"}}]
  (let [instant-src (or instant-src (->mock-clock))]
    (node/start-node {:xtdb.log/local-directory-log {:root-path (.resolve node-dir "log")
                                                     :instant-src instant-src}
                      :xtdb.tx-producer/tx-producer {:instant-src instant-src}
                      :xtdb.buffer-pool/buffer-pool {:cache-path (.resolve node-dir buffers-dir)}
                      :xtdb.object-store/file-system-object-store {:root-path (.resolve node-dir "objects")}
                      :xtdb/indexer (->> {:rows-per-chunk rows-per-chunk}
                                         (into {} (filter val)))})))

(defn ->local-submit-node ^java.lang.AutoCloseable [{:keys [^Path node-dir]}]
  (node/start-submit-node {:xtdb.tx-producer/tx-producer {:clock (->mock-clock)}
                           :xtdb.log/local-directory-log {:root-path (.resolve node-dir "log")
                                                           :clock (->mock-clock)}}))

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
