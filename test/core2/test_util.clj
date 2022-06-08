(ns core2.test-util
  (:require [cheshire.core :as json]
            [clojure.test :as t]
            [core2.api :as c2]
            [core2.json :as c2-json]
            [core2.local-node :as node]
            core2.object-store
            [core2.temporal :as temporal]
            [core2.types :as ty]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [core2.logical-plan :as lp]
            [clojure.spec.alpha :as s]
            [core2.types :as types])
  (:import core2.ICursor
           core2.local_node.Node
           core2.object_store.FileSystemObjectStore
           core2.vector.IIndirectVector
           java.net.ServerSocket
           [java.nio.file Files Path]
           java.nio.file.attribute.FileAttribute
           [java.time Clock Duration Instant Period ZoneId]
           [java.util ArrayList LinkedList]
           java.util.concurrent.TimeUnit
           java.util.function.Consumer
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           [org.apache.arrow.vector FieldVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo ArrowType Field FieldType Schema]))

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

(defn component
  ([k] (component *node* k))
  ([node k] (util/component node k)))

(defn ->list ^java.util.List [^ValueVector v]
  (let [acc (ArrayList.)]
    (dotimes [n (.getValueCount v)]
      (.add acc (ty/get-object v n)))
    acc))

(defn then-await-tx
  (^core2.api.TransactionInstant [tx node]
   @(node/await-tx-async node tx))

  (^core2.api.TransactionInstant [tx node ^Duration timeout]
   @(-> (node/await-tx-async node tx)
        (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS))))

(defn latest-completed-tx ^core2.api.TransactionInstant [node]
  (:latest-completed-tx (c2/status node)))

(defn ->mock-clock
  (^java.time.Clock []
   (->mock-clock (iterate #(.plus ^Instant % (Period/ofDays 1))
                          (.toInstant #inst "2020-01-01"))))

  (^java.time.Clock [^Iterable insts]
   (let [times-iterator (.iterator insts)]
     (proxy [Clock] []
       (getZone []
         (ZoneId/of "UTC"))
       (instant []
         (if (.hasNext times-iterator)
           ^Instant (.next times-iterator)
           (throw (IllegalStateException. "out of time"))))))))

(defn await-temporal-snapshot-build [^Node node]
  (.awaitSnapshotBuild ^core2.temporal.TemporalManagerPrivate (::temporal/temporal-manager @(:!system node))))

(defn finish-chunk [^Node node]
  (.finishChunk ^core2.indexer.Indexer (.indexer node))
  (await-temporal-snapshot-build node))

(defn write-duv! [^DenseUnionVector duv, vs]
  (.clear duv)

  (let [writer (-> (vw/vec->writer duv) .asDenseUnion)]
    (doseq [v vs]
      (.startValue writer)
      (doto (.writerForType writer (ty/value->col-type v))
        (.startValue)
        (->> (ty/write-value! v))
        (.endValue))
      (.endValue writer))

    (util/set-value-count duv (count vs))

    duv))

(defn ->duv ^org.apache.arrow.vector.complex.DenseUnionVector [col-name vs]
  (let [res (.createVector (ty/->field col-name ty/dense-union-type false) *allocator*)]
    (try
      (doto res (write-duv! vs))
      (catch Exception e
        (.close res)
        (throw e)))))

(defn write-vec! [^ValueVector v, vs]
  (.clear v)

  (let [writer (vw/vec->writer v)]
    (doseq [v vs]
      (doto writer
        (.startValue)
        (->> (ty/write-value! v))
        (.endValue)))

    (util/set-value-count v (count vs))

    v))

(defn ->mono-vec
  (^org.apache.arrow.vector.ValueVector [^String col-name col-type]
   (let [^FieldType field-type (cond
                                 (instance? FieldType col-type) col-type
                                 (instance? ArrowType col-type) (FieldType/notNullable col-type)
                                 :else (throw (UnsupportedOperationException.)))]
     (.createNewSingleVector field-type col-name *allocator* nil)))

  (^org.apache.arrow.vector.ValueVector [^String col-name col-type vs]
   (let [res (->mono-vec col-name col-type)]
     (try
       (doto res (write-vec! vs))
       (catch Exception e
         (.close res)
         (throw e))))))

(defn populate-root ^core2.vector.IIndirectRelation [^VectorSchemaRoot root rows]
  (.clear root)

  (let [field-vecs (.getFieldVectors root)
        row-count (count rows)]
    (doseq [^FieldVector field-vec field-vecs]
      (write-vec! field-vec (map (keyword (.getName (.getField field-vec))) rows)))

    (util/set-vector-schema-root-row-count root row-count)

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
         :schema #(instance? Schema %)
         :blocks vector?))

(defmethod lp/emit-expr ::blocks [{:keys [^Schema schema blocks]} _args]
  {:col-types (->> (.getFields schema)
                   (into {} (map (juxt #(symbol (.getName ^Field %)) types/field->col-type))))
   :->cursor (fn [{:keys [allocator]}]
               (->cursor allocator schema blocks))})

(defn <-column [^IIndirectVector col]
  (mapv (fn [idx]
          (ty/get-object (.getVector col) (.getIndex col idx)))
        (range (.getValueCount col))))

(defn <-cursor [^ICursor cursor]
  (let [!res (volatile! (transient []))]
    (.forEachRemaining cursor
                       (reify Consumer
                         (accept [_ rel]
                           (vswap! !res conj! (iv/rel->rows rel)))))
    (persistent! @!res)))

(t/deftest round-trip-cursor
  (with-allocator
    (fn []
      (let [blocks [[{:name "foo", :age 20}
                     {:name "bar", :age 25}]
                    [{:name "baz", :age 30}]]]
        (with-open [cursor (->cursor (Schema. [(ty/->field "name" ty/varchar-type false)
                                               (ty/->field "age" ty/bigint-type false)])
                                     blocks)]

          (t/is (= blocks (<-cursor cursor))))))))

(defn check-json-file [^Path expected, ^Path actual]
  (t/is (= (json/parse-string (Files/readString expected))
           (json/parse-string (Files/readString actual)))
        actual))

(defn check-json [^Path expected-path, ^FileSystemObjectStore os]
  (let [^Path os-path (.root-path os)]

    (let [expected-file-count (.count (Files/list expected-path))]
      (t/is (= expected-file-count (.count (Files/list os-path))))
      (t/is (= expected-file-count (count (.listObjects os)))))

    (c2-json/write-arrow-json-files (.toFile os-path))

    (doseq [^Path path (iterator-seq (.iterator (Files/list os-path)))
            :when (.endsWith (str path) ".json")]
      (check-json-file (.resolve expected-path (.getFileName path)) path))))

(defn ->local-node ^core2.local_node.Node [{:keys [^Path node-dir
                                             clock max-rows-per-block max-rows-per-chunk buffers-dir]
                                      :or {buffers-dir "buffers"}}]
  (node/start-node {:core2.log/local-directory-log (cond-> {:root-path (.resolve node-dir "log")}
                                                     clock (assoc :clock clock))
                    :core2.buffer-pool/buffer-pool {:cache-path (.resolve node-dir ^String buffers-dir)}
                    :core2.object-store/file-system-object-store {:root-path (.resolve node-dir "objects")}
                    :core2.indexer/indexer (->> {:max-rows-per-block max-rows-per-block
                                                 :max-rows-per-chunk max-rows-per-chunk}
                                                (into {} (filter val)))}))

(defn ->local-submit-node ^core2.local_node.SubmitNode [{:keys [^Path node-dir clock]}]
  (node/start-submit-node {:core2.log/local-directory-log (cond-> {:root-path (.resolve node-dir "log")}
                                                            clock (assoc :clock clock))}))

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

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))
