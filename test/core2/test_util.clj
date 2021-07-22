(ns core2.test-util
  (:require [cheshire.core :as json]
            [clojure.test :as t]
            [core2.core :as c2]
            [core2.json :as c2-json]
            core2.object-store
            [core2.relation :as rel]
            [core2.types :as ty]
            [core2.util :as util]
            [core2.temporal :as temporal])
  (:import core2.core.Node
           core2.ICursor
           core2.object_store.FileSystemObjectStore
           core2.relation.IReadColumn
           [java.nio.file Files Path]
           java.nio.file.attribute.FileAttribute
           [java.time Clock Duration ZoneId]
           [java.util ArrayList Date LinkedList]
           java.util.concurrent.TimeUnit
           java.util.function.Consumer
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector FieldVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.types.pojo.Schema))

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
  (with-open [node (c2/start-node *node-opts*)]
    (binding [*node* node]
      (f))))

(defn ->list ^java.util.List [^ValueVector v]
  (let [acc (ArrayList.)]
    (dotimes [n (.getValueCount v)]
      (.add acc (ty/get-object v n)))
    acc))

(defn then-await-tx
  (^core2.tx.TransactionInstant [tx node]
   @(c2/await-tx-async node tx))

  (^core2.tx.TransactionInstant [tx node ^Duration timeout]
   @(-> (c2/await-tx-async node tx)
        (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS))))

(defn ->mock-clock ^java.time.Clock [^Iterable dates]
  (let [times-iterator (.iterator dates)]
    (proxy [Clock] []
      (getZone []
        (ZoneId/of "UTC"))
      (instant []
        (if (.hasNext times-iterator)
          (.toInstant ^Date (.next times-iterator))
          (throw (IllegalStateException. "out of time")))))))

(defn await-temporal-snapshot-build [^Node node]
  (.awaitSnapshotBuild ^core2.temporal.TemporalManagerPrivate (::temporal/temporal-manager @(:!system node))))

(defn finish-chunk [^Node node]
  (.finishChunk ^core2.indexer.Indexer (.indexer node))
  (await-temporal-snapshot-build node))

(defn populate-root ^core2.relation.IReadRelation [^VectorSchemaRoot root rows]
  (.clear root)

  (let [field-vecs (.getFieldVectors root)
        row-count (count rows)]
    (.setRowCount root row-count)
    (doseq [^FieldVector field-vec field-vecs]
      (dotimes [idx row-count]
        (ty/set-safe! field-vec idx (-> (nth rows idx)
                                        (get (keyword (.getName (.getField field-vec))))))))
    root))

(defn ->relation ^core2.relation.IReadRelation [schema rows]
  (let [root (VectorSchemaRoot/create schema *allocator*)]
    (populate-root root rows)
    (rel/<-root root)))

(defn ->cursor ^core2.ICursor [schema blocks]
  (let [blocks (LinkedList. blocks)
        root (VectorSchemaRoot/create schema *allocator*)]
    (reify ICursor
      (tryAdvance [_ c]
        (if-let [block (some-> (.poll blocks) vec)]
          (do
            (populate-root root block)
            (.accept c (rel/<-root root))
            true)
          false))

      (close [_]
        (.close root)))))

(defn <-column [^IReadColumn col]
  (mapv (fn [idx]
          (.getObject col idx))
        (range (.valueCount col))))

(defn <-cursor [^ICursor cursor]
  (let [!res (volatile! (transient []))]
    (.forEachRemaining cursor
                       (reify Consumer
                         (accept [_ rel]
                           (vswap! !res conj! (vec (rel/rel->rows rel))))))
    (persistent! @!res)))

(t/deftest round-trip-cursor
  (with-allocator
    (fn []
      (let [blocks [[{:name "foo", :age 20}
                     {:name "bar", :age 25}]
                    [{:name "baz", :age 30}]]]
        (with-open [cursor (->cursor (Schema. [(ty/->field "name" (ty/->arrow-type :varchar) false)
                                               (ty/->field "age" (ty/->arrow-type :bigint) false)])
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

(defn ->local-node ^core2.core.Node [{:keys [^Path node-dir
                                             clock max-rows-per-block max-rows-per-chunk buffers-dir]
                                      :or {buffers-dir "buffers"}}]
  (c2/start-node {:core2.log/local-directory-log (cond-> {:root-path (.resolve node-dir "log")}
                                                   clock (assoc :clock clock))
                  :core2.buffer-pool/buffer-pool {:cache-path (.resolve node-dir ^String buffers-dir)}
                  :core2.object-store/file-system-object-store {:root-path (.resolve node-dir "objects")}
                  :core2.indexer/indexer (->> {:max-rows-per-block max-rows-per-block
                                               :max-rows-per-chunk max-rows-per-chunk}
                                              (into {} (filter val)))}))

(defn ->local-submit-node ^core2.core.SubmitNode [{:keys [^Path node-dir clock]}]
  (c2/start-submit-node {:core2.log/local-directory-log (cond-> {:root-path (.resolve node-dir "log")}
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
