(ns core2.test-util
  (:require [cheshire.core :as json]
            [clojure.test :as t]
            [core2.core :as c2]
            [core2.json :as c2-json]
            [core2.types :as ty]
            [core2.util :as util])
  (:import core2.core.Node
           [core2 IChunkCursor ICursor]
           core2.object_store.FileSystemObjectStore
           [java.nio.file Files Path]
           java.nio.file.attribute.FileAttribute
           [java.time Clock Duration ZoneId]
           [java.util ArrayList Date LinkedList]
           java.util.concurrent.CompletableFuture
           java.util.function.Consumer
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector FieldVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.types.Types$MinorType))

(def ^:dynamic ^org.apache.arrow.memory.BufferAllocator *allocator*)

(defn with-allocator [f]
  (with-open [allocator (RootAllocator.)]
    (binding [*allocator* allocator]
      (f))))

(defn root->rows [^VectorSchemaRoot root]
  (let [field-vecs (.getFieldVectors root)]
    (mapv (fn [idx]
            (vec (for [^FieldVector field-vec field-vecs]
                   (ty/get-object field-vec idx))))
          (range (.getRowCount root)))))

(defn ->list ^java.util.List [^ValueVector v]
  (let [acc (ArrayList.)]
    (dotimes [n (.getValueCount v)]
      (.add acc (ty/get-object v n)))
    acc))

(defn then-await-tx
  ([^CompletableFuture submit-tx-fut, ^Node node]
   (-> submit-tx-fut
       (then-await-tx node (Duration/ofSeconds 2))))

  ([^CompletableFuture submit-tx-fut, ^Node node, timeout]
   (-> submit-tx-fut
       (util/then-apply
        (fn [tx]
          (c2/await-tx node tx timeout))))))

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
  (.awaitSnapshotBuild ^core2.temporal.TemporalManagerPrivate (:core2/temporal-manager @(:!system node))))

(defn finish-chunk [^Node node]
  (.finishChunk ^core2.indexer.Indexer (.indexer node))
  (await-temporal-snapshot-build node))

(defn ->cursor ^core2.IChunkCursor [schema blocks]
  (let [blocks (LinkedList. blocks)
        root (VectorSchemaRoot/create schema *allocator*)]
    (reify IChunkCursor
      (getSchema [_] schema)

      (tryAdvance [_ c]
        (if-let [block (some-> (.poll blocks) vec)]
          (do
            (.clear root)
            (let [field-vecs (.getFieldVectors root)
                  row-count (count block)]
              (.setRowCount root row-count)
              (doseq [^FieldVector field-vec field-vecs]
                (dotimes [idx row-count]
                  (ty/set-safe! field-vec idx (-> (nth block idx)
                                                  (get (keyword (.getName (.getField field-vec))))))))
              (.accept c root)
              true))
          false))

      (close [_]
        (.close root)))))

(defn <-cursor [^ICursor cursor]
  (let [!res (volatile! (transient []))]
    (.forEachRemaining cursor
                       (reify Consumer
                         (accept [_ root]
                           (let [ks (->> (.getFields (.getSchema ^VectorSchemaRoot root))
                                         (into [] (map (comp keyword #(.getName ^Field %)))))]
                             (vswap! !res conj! (mapv (fn [row] (zipmap ks row)) (root->rows root)))))))
    (persistent! @!res)))

(t/deftest round-trip-cursor
  (with-allocator
    (fn []
      (let [blocks [[{:name "foo", :age 20}
                     {:name "bar", :age 25}]
                    [{:name "baz", :age 30}]]]
        (with-open [cursor (->cursor (Schema. [(ty/->field "name" (.getType Types$MinorType/VARCHAR) false)
                                               (ty/->field "age" (.getType Types$MinorType/BIGINT) false)])
                                     blocks)]

          (t/is (= (<-cursor cursor) blocks)))))))

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
                                             clock max-rows-per-block max-rows-per-chunk buffers-dir compress-snapshots?]
                                      :or {buffers-dir "buffers"
                                           compress-snapshots? true}}]
  (c2/start-node {:core2/log (cond-> {:core2/module 'core2.log/->local-directory-log
                                      :root-path (.resolve node-dir "log")}
                               clock (assoc :clock clock))
                  :core2/buffer-pool {:cache-path (.resolve node-dir ^String buffers-dir)}
                  :core2/temporal-manager {:compress-snapshots? compress-snapshots?}
                  :core2/object-store {:core2/module 'core2.object-store/->file-system-object-store
                                       :root-path (.resolve node-dir "objects")}
                  :core2/indexer (->> {:max-rows-per-block max-rows-per-block
                                       :max-rows-per-chunk max-rows-per-chunk}
                                      (into {} (filter val)))}))

(defn ->local-submit-node ^core2.core.SubmitNode [{:keys [^Path node-dir clock]}]
  (c2/start-submit-node {:core2/log (cond-> {:root-path (.resolve node-dir "log")}
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
