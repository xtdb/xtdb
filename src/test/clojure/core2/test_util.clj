(ns core2.test-util
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [core2.api.impl :as api]
            [core2.client :as client]
            [core2.indexer :as idx]
            [core2.logical-plan :as lp]
            [core2.node :as node]
            core2.object-store
            [core2.operator :as op]
            core2.operator.scan
            [core2.temporal :as temporal]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [time-literals.read-write :as time-literals])
  (:import (core2 ICursor InstantSource)
           core2.node.Node
           core2.operator.scan.ScanSource
           (core2.vector IIndirectRelation IIndirectVector)
           java.net.ServerSocket
           (java.nio.file Files Path)
           java.nio.file.attribute.FileAttribute
           (java.time Duration Instant Period)
           (java.util LinkedList)
           java.util.function.Consumer
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector FieldVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo Schema)))

(time-literals/print-time-literals-clj!)

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
                                       (assoc-in [:core2/server :port] port)))]
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

(defn then-await-tx
  (^core2.operator.scan.ScanSource [tx node]
   (.txBasis ^ScanSource @(node/snapshot-async node tx)))

  (^core2.operator.scan.ScanSource [tx node ^Duration timeout]
   (.txBasis ^ScanSource @(node/snapshot-async node tx timeout))))

(defn latest-completed-tx ^core2.api.TransactionInstant [node]
  (:latest-completed-tx (api/status node)))

(defn ->mock-clock
  (^core2.InstantSource []
   (->mock-clock (iterate #(.plus ^Instant % (Period/ofDays 1))
                          (.toInstant #inst "2020-01-01"))))

  (^core2.InstantSource [^Iterable insts] (InstantSource/mock insts)))

(defn with-mock-clock [f]
  (with-opts {:core2.log/memory-log {:instant-src (->mock-clock)}} f))

(defn await-temporal-snapshot-build [^Node node]
  (.awaitSnapshotBuild ^core2.temporal.TemporalManagerPrivate (::temporal/temporal-manager @(:!system node))))

(defn finish-chunk! [node]
  (idx/finish-block! (component node :core2/indexer))
  (idx/finish-chunk! (component node :core2/indexer))
  (await-temporal-snapshot-build node))

(defn open-vec
  (^org.apache.arrow.vector.ValueVector [col-name vs]
   (vw/open-vec *allocator* col-name vs))

  (^org.apache.arrow.vector.ValueVector [col-name col-type vs]
   (vw/open-vec *allocator* col-name col-type vs)))

(defn open-rel ^core2.vector.IIndirectRelation [vecs]
  (vw/open-rel vecs))

(defn open-params ^core2.vector.IIndirectRelation [params-map]
  (vw/open-params *allocator* params-map))

(defn populate-root ^core2.vector.IIndirectRelation [^VectorSchemaRoot root rows]
  (.clear root)

  (let [field-vecs (.getFieldVectors root)
        row-count (count rows)]
    (doseq [^FieldVector field-vec field-vecs]
      (vw/write-vec! field-vec (map (keyword (.getName (.getField field-vec))) rows)))

    (.setRowCount root row-count)
    root))

(defn ->cursor
  (^core2.ICursor [^Schema schema, blocks] (->cursor *allocator* schema blocks))

  (^core2.ICursor [^BufferAllocator allocator ^Schema schema, blocks]
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
                      (types/rows->col-types (into [] cat blocks)))
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
  ([query {:keys [params preserve-blocks? with-col-types?] :as query-opts}]
   (with-open [^IIndirectRelation
               params-rel (iv/->indirect-rel (for [[k v] params]
                                               (iv/->direct-vec (open-vec k [v])))
                                             1)]
     (let [pq (op/prepare-ra query)
           bq (.bind pq (-> (select-keys query-opts [:srcs :current-time :default-tz :table-args])
                            (assoc :params params-rel)))]
       (with-open [res (.openCursor bq)]
         (let [rows (-> (<-cursor res)
                        (cond->> (not preserve-blocks?) (into [] cat)))]
           (if with-col-types?
             {:res rows, :col-types (.columnTypes bq)}
             rows)))))))

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

(defn ->local-node ^core2.node.Node [{:keys [^Path node-dir ^String buffers-dir
                                             rows-per-block rows-per-chunk]
                                      :or {buffers-dir "buffers"}}]
  (node/start-node {:core2.log/local-directory-log {:root-path (.resolve node-dir "log")
                                                    :instant-src (->mock-clock)}
                    :core2.tx-producer/tx-producer {:instant-src (->mock-clock)}
                    :core2.buffer-pool/buffer-pool {:cache-path (.resolve node-dir buffers-dir)}
                    :core2.object-store/file-system-object-store {:root-path (.resolve node-dir "objects")}
                    :core2/live-chunk (->> {:rows-per-block rows-per-block
                                            :rows-per-chunk rows-per-chunk}
                                           (into {} (filter val)))}))

(defn ->local-submit-node ^core2.node.SubmitNode [{:keys [^Path node-dir]}]
  (node/start-submit-node {:core2.tx-producer/tx-producer {:clock (->mock-clock)}
                           :core2.log/local-directory-log {:root-path (.resolve node-dir "log")
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
