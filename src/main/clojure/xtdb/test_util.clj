(ns xtdb.test-util
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [xtdb.api.protocols :as api]
            [xtdb.client :as client]
            [xtdb.indexer :as idx]
            xtdb.ingester
            [xtdb.logical-plan :as lp]
            [xtdb.node :as node]
            xtdb.object-store
            [xtdb.operator :as op]
            xtdb.operator.scan
            [xtdb.temporal :as temporal]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.indirect :as iv]
            [xtdb.vector.writer :as vw])
  (:import [ch.qos.logback.classic Level Logger]
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
           xtdb.ingester.Ingester
           (xtdb.operator IRaQuerySource PreparedQuery)
           (xtdb.vector IIndirectRelation IIndirectVector)))

(def ^:dynamic ^org.apache.arrow.memory.BufferAllocator *allocator*)

(defn with-allocator [f]
  (with-open [allocator (RootAllocator.)]
    (binding [*allocator* allocator]
      (f))))

(def ^:dynamic ^:private *node-opts* {})
(def ^:dynamic *node*)

(defn with-opts
  ([opts] (partial with-opts opts))
  ([opts f]
   (binding [*node-opts* (merge *node-opts* opts)]
     (f))))

(defn with-node [f]
  (with-open [node (node/start-node *node-opts*)]
    (binding [*node* node]
      (f))))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn with-http-client-node [f]
  (let [port (free-port)]
    (with-open [_ (node/start-node (-> *node-opts*
                                       (assoc-in [:xtdb/server :port] port)))]
      (binding [*node* (client/start-client (str "http://localhost:" port))]
        (f)))))

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

(defn then-await-tx*
  (^xtdb.api.TransactionInstant [tx node]
   (then-await-tx* tx node nil))

  (^xtdb.api.TransactionInstant [tx node ^Duration timeout]
   @(.awaitTxAsync ^Ingester (util/component node :xtdb/ingester) tx timeout)))

(defn latest-completed-tx ^xtdb.api.TransactionInstant [node]
  (:latest-completed-tx (api/status node)))

(defn ->mock-clock
  (^xtdb.InstantSource []
   (->mock-clock (iterate #(.plus ^Instant % (Period/ofDays 1))
                          (.toInstant #inst "2020-01-01"))))

  (^xtdb.InstantSource [^Iterable insts] (InstantSource/mock insts)))

(defn with-mock-clock [f]
  (with-opts {:xtdb.log/memory-log {:instant-src (->mock-clock)}} f))

(defn await-temporal-snapshot-build [node]
  (.awaitSnapshotBuild ^xtdb.temporal.TemporalManagerPrivate (util/component node ::temporal/temporal-manager)))

(defn finish-chunk! [node]
  (idx/finish-block! (component node :xtdb/indexer))
  (idx/finish-chunk! (component node :xtdb/indexer))
  (await-temporal-snapshot-build node))

(defn open-vec
  (^org.apache.arrow.vector.ValueVector [col-name vs]
   (vw/open-vec *allocator* col-name vs))

  (^org.apache.arrow.vector.ValueVector [col-name col-type vs]
   (vw/open-vec *allocator* col-name col-type vs)))

(defn open-rel ^xtdb.vector.IIndirectRelation [vecs]
  (vw/open-rel vecs))

(defn open-params ^xtdb.vector.IIndirectRelation [params-map]
  (vw/open-params *allocator* params-map))

(defn populate-root ^xtdb.vector.IIndirectRelation [^VectorSchemaRoot root rows]
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
             (.accept c (iv/<-root root))
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

(defn <-column [^IIndirectVector col]
  (mapv (fn [idx]
          (types/get-object (.getVector col) (.getIndex col idx)))
        (range (.getValueCount col))))

(defn <-cursor [^ICursor cursor]
  (let [!res (volatile! (transient []))]
    (.forEachRemaining cursor
                       (reify Consumer
                         (accept [_ rel]
                           (vswap! !res conj! (iv/rel->rows rel)))))
    (persistent! @!res)))

(defn query-ra
  ([query] (query-ra query {}))
  ([query {:keys [node params preserve-blocks? with-col-types?] :as query-opts}]
   (let [^IIndexer indexer (util/component node :xtdb/indexer)
         query-opts (cond-> query-opts
                      node (-> (update :basis api/after-latest-submitted-tx node)
                               (doto (-> :basis :after-tx (then-await-tx* node)))))]

     (with-open [^IIndirectRelation
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
                                                     rows-per-block rows-per-chunk]
                                              :or {buffers-dir "buffers"}}]
  (node/start-node {:xtdb.log/local-directory-log {:root-path (.resolve node-dir "log")
                                                    :instant-src (->mock-clock)}
                    :xtdb.tx-producer/tx-producer {:instant-src (->mock-clock)}
                    :xtdb.buffer-pool/buffer-pool {:cache-path (.resolve node-dir buffers-dir)}
                    :xtdb.object-store/file-system-object-store {:root-path (.resolve node-dir "objects")}
                    :xtdb/live-chunk (->> {:rows-per-block rows-per-block
                                            :rows-per-chunk rows-per-chunk}
                                           (into {} (filter val)))}))

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

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defmacro with-log-level [ns level & body]
  `(let [level# (get-log-level! ~ns)]
     (try
       (set-log-level! ~ns ~level)
       ~@body
       (finally
         (set-log-level! ~ns level#)))))
