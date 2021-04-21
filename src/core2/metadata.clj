(ns core2.metadata
  (:require [core2.bloom :as bloom]
            core2.buffer-pool
            [core2.expression.comparator :as expr.comp]
            core2.object-store
            [core2.system :as sys]
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.buffer_pool.IBufferPool
           core2.object_store.ObjectStore
           core2.tx.Watermark
           java.io.Closeable
           [java.util Date List SortedSet]
           [java.util.concurrent CompletableFuture ConcurrentSkipListSet]
           java.util.function.Consumer
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector FieldVector TimeStampMilliVector VarBinaryVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType Schema]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(definterface IMetadataManager
  (^void registerNewChunk [^java.util.Map roots, ^long chunk-idx])
  (^java.util.SortedSet knownChunks [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^java.util.function.Function f]))

(defn type->field-name [^ArrowType arrow-type]
  (-> (Types/getMinorTypeForArrowType arrow-type)
      (.toString)
      (.toLowerCase)))

(def ^:private type-meta-fields
  (for [arrow-type (keys t/arrow-type->vector-type)]
    (t/->field (type->field-name arrow-type) arrow-type true)))

(def ^org.apache.arrow.vector.types.pojo.Schema metadata-schema
  (Schema. [(t/->field "column" (.getType Types$MinorType/VARCHAR) false)
            (t/->field "count" (.getType Types$MinorType/BIGINT) false)
            (t/->field "min-row-id" (.getType Types$MinorType/BIGINT) false)
            (t/->field "max-row-id" (.getType Types$MinorType/BIGINT) false)

            (apply t/->field "min"
                   (.getType Types$MinorType/STRUCT) false
                   type-meta-fields)
            (apply t/->field "max"
                   (.getType Types$MinorType/STRUCT) false
                   type-meta-fields)
            (t/->field "bloom" (.getType Types$MinorType/VARBINARY) false)]))

(defn- ->metadata-obj-key [chunk-idx]
  (format "metadata-%08x.arrow" chunk-idx))

(defn ->chunk-obj-key [chunk-idx column-name]
  (format "chunk-%08x-%s.arrow" chunk-idx column-name))

(defn- obj-key->chunk-idx [obj-key]
  (some-> (second (re-matches #"metadata-(\p{XDigit}{8}).arrow" obj-key))
          (Long/parseLong 16)))

(defn- write-min-max [^FieldVector field-vec,
                      ^StructVector min-meta-vec, ^StructVector max-meta-vec,
                      ^long meta-idx]
  (when (pos? (.getValueCount field-vec))
    (let [arrow-type (.getType (.getField field-vec))
          type-name (type->field-name arrow-type)
          vec-comparator (expr.comp/->comparator arrow-type)

          min-vec (.getChild min-meta-vec type-name)
          max-vec (.getChild max-meta-vec type-name)]

      (dotimes [field-vec-idx (.getValueCount field-vec)]
        (when (or (.isNull min-vec meta-idx)
                  (and (not (.isNull field-vec field-vec-idx))
                       (neg? (.compareIdx vec-comparator field-vec field-vec-idx min-vec meta-idx))))
          (.copyFromSafe min-vec field-vec-idx meta-idx field-vec))

        (when (or (.isNull max-vec meta-idx)
                  (and (not (.isNull field-vec field-vec-idx))
                       (pos? (.compareIdx vec-comparator field-vec field-vec-idx max-vec meta-idx))))
          (.copyFromSafe max-vec field-vec-idx meta-idx field-vec))))))

(defn write-meta [^VectorSchemaRoot metadata-root, live-roots]
  (let [col-count (count live-roots)

        ^VarCharVector column-name-vec (.getVector metadata-root "column")
        ^BigIntVector count-vec (.getVector metadata-root "count")
        ^BigIntVector min-row-id-vec (.getVector metadata-root "min-row-id")
        ^BigIntVector max-row-id-vec (.getVector metadata-root "max-row-id")

        ^StructVector min-vec (.getVector metadata-root "min")
        ^StructVector max-vec (.getVector metadata-root "max")
        ^VarBinaryVector bloom-vec (.getVector metadata-root "bloom")]

    (.setRowCount metadata-root col-count)

    (dorun
     (->> (sort-by key live-roots)
          (map-indexed
           (fn [^long column-idx [^String col-name, ^VectorSchemaRoot live-root]]
             (let [row-count (.getRowCount live-root)
                   ^DenseUnionVector column-vec (.getVector live-root col-name)
                   ^BigIntVector row-id-vec (.getVector live-root "_row-id")]
               (.setSafe column-name-vec column-idx (Text. col-name))
               (.setSafe count-vec column-idx row-count)
               (.setSafe min-row-id-vec column-idx (.get row-id-vec 0))
               (.setSafe max-row-id-vec column-idx (.get row-id-vec (dec row-count)))

               (.setIndexDefined min-vec column-idx)
               (.setIndexDefined max-vec column-idx)

               (doseq [child-vec (.getChildrenFromFields column-vec)]
                 (write-min-max child-vec min-vec max-vec column-idx))

               (bloom/write-bloom bloom-vec column-idx column-vec))))))

    ;; we set row-count twice in this function
    ;; - at the beginning to allocate memory
    ;; - at the end to finalise the offset vectors of the variable width vectors
    ;; there may be a more idiomatic way to achieve this
    (.setRowCount metadata-root col-count)))

(defn metadata-column-idx [^VectorSchemaRoot metadata-root, ^String col-name]
  (let [col-count (.getRowCount metadata-root)
        ^VarCharVector column-name-vector (.getVector metadata-root "column")]
    (loop [idx 0]
      (when (< idx col-count)
        (if (= col-name (str (.getObject column-name-vector idx)))
          idx
          (recur (inc idx)))))))

(defn with-metadata [^IMetadataManager metadata-mgr, ^long chunk-idx, f]
  (.withMetadata metadata-mgr chunk-idx (util/->jfn f)))

(defn with-latest-metadata [^IMetadataManager metadata-mgr, f]
  (if-let [chunk-idx (last (.knownChunks metadata-mgr))]
    (with-metadata metadata-mgr chunk-idx f)
    (CompletableFuture/completedFuture nil)))

(defn latest-tx [^VectorSchemaRoot metadata-root]
  (let [^StructVector max-vec (.getVector metadata-root "max")
        tx-id-idx (metadata-column-idx metadata-root "_tx-id")
        tx-time-idx (metadata-column-idx metadata-root "_tx-time")]
    (tx/->TransactionInstant (-> max-vec
                                 ^BigIntVector
                                 (.getChild (type->field-name (.getType Types$MinorType/BIGINT)))
                                 (.get tx-id-idx))
                             (Date. (-> max-vec
                                        ^TimeStampMilliVector
                                        (.getChild (type->field-name (.getType Types$MinorType/TIMESTAMPMILLI)))
                                        (.get tx-time-idx))))))

(defn latest-row-id [^VectorSchemaRoot metadata-root]
  (.get ^BigIntVector (.getVector metadata-root "max-row-id")
        (metadata-column-idx metadata-root "_tx-id")))

(defn matching-chunks [^IMetadataManager metadata-mgr, ^Watermark watermark, metadata-pred]
  (->> (for [^long chunk-idx (.knownChunks metadata-mgr)
             :while (or (nil? watermark) (< chunk-idx (.chunk-idx watermark)))]
         (MapEntry/create chunk-idx (with-metadata metadata-mgr chunk-idx metadata-pred)))
       vec (filter (comp deref val)) keys))

(deftype MetadataManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^IBufferPool buffer-pool
                          ^SortedSet known-chunks]
  IMetadataManager
  (registerNewChunk [_ live-roots chunk-idx]
    (let [metadata-buf (with-open [metadata-root (VectorSchemaRoot/create metadata-schema allocator)]
                         (write-meta metadata-root live-roots)

                         (util/root->arrow-ipc-byte-buffer metadata-root :file))]

      @(.putObject object-store (->metadata-obj-key chunk-idx) metadata-buf)

      (.add known-chunks chunk-idx)))

  (withMetadata [_ chunk-idx f]
    (-> (.getBuffer buffer-pool (->metadata-obj-key chunk-idx))
        (util/then-apply
          (fn [^ArrowBuf metadata-buffer]
            (assert metadata-buffer)

            (when metadata-buffer
              (let [res (promise)]
                (try
                  (with-open [chunk (util/->chunks metadata-buffer allocator)]
                    (.tryAdvance chunk
                                 (reify Consumer
                                   (accept [_ metadata-root]
                                     (deliver res (.apply f metadata-root))))))

                  (assert (realized? res))
                  @res

                  (finally
                    (.close metadata-buffer)))))))))

  (knownChunks [_] known-chunks)

  Closeable
  (close [_]
    (.clear known-chunks)))

(defn ->metadata-manager {::sys/deps {:allocator :core2/allocator
                                      :object-store :core2/object-store
                                      :buffer-pool :core2/buffer-pool}}
  [{:keys [allocator ^ObjectStore object-store buffer-pool]}]
  (MetadataManager. allocator object-store buffer-pool
                    (ConcurrentSkipListSet. ^List (keep obj-key->chunk-idx (.listObjects object-store "metadata-")))))
