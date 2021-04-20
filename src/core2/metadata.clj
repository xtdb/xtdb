(ns core2.metadata
  (:require core2.buffer-pool
            [core2.expression :as expr]
            [core2.system :as sys]
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util]
            [core2.bloom :as bloom])
  (:import clojure.lang.MapEntry
           core2.buffer_pool.IBufferPool
           core2.object_store.ObjectStore
           core2.tx.Watermark
           core2.types.ReadWrite
           java.io.Closeable
           [java.util Comparator Date List SortedSet]
           [java.util.concurrent CompletableFuture ConcurrentSkipListSet]
           java.util.function.Consumer
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector BitVector FieldVector TinyIntVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector]
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableTimeStampMilliHolder ValueHolder]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType$FixedSizeList Schema]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(definterface IMetadataManager
  (^void registerNewChunk [^java.util.Map roots, ^long chunk-idx])
  (^java.util.SortedSet knownChunks [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^java.util.function.Function f]))

(def ^org.apache.arrow.vector.types.pojo.Field column-name-field (t/->field "column" (.getType Types$MinorType/VARCHAR) false))
(def ^org.apache.arrow.vector.types.pojo.Field field-name-field (t/->field "field" (.getType Types$MinorType/VARCHAR) false))
(def ^org.apache.arrow.vector.types.pojo.Field type-id-field (t/->field "type-id" (.getType Types$MinorType/TINYINT) false))

(def ^org.apache.arrow.vector.types.pojo.Schema metadata-schema
  (Schema. [column-name-field
            field-name-field
            type-id-field
            (t/->primitive-dense-union-field "min")
            (t/->primitive-dense-union-field "max")
            (t/->field "count" (.getType Types$MinorType/BIGINT) false)
            (t/->field "bloom" (ArrowType$FixedSizeList. bloom/bloom-bits) false
                       (t/->field "bloom-bits" (.getType Types$MinorType/BIT) false))]))

(defn- ->metadata-obj-key [chunk-idx]
  (format "metadata-%08x.arrow" chunk-idx))

(defn ->chunk-obj-key [chunk-idx column-name]
  (format "chunk-%08x-%s.arrow" chunk-idx column-name))

(defn- obj-key->chunk-idx [obj-key]
  (some-> (second (re-matches #"metadata-(\p{XDigit}{8}).arrow" obj-key))
          (Long/parseLong 16)))

(definterface MinMax
  (^void writeMinMax [^org.apache.arrow.vector.FieldVector src-vec
                      ^org.apache.arrow.vector.FieldVector min-vec, ^int min-vec-idx
                      ^org.apache.arrow.vector.FieldVector max-vec, ^int max-vec-idx]))

(deftype MinMaxImpl [^ReadWrite rw, ^Comparator comparator
                     curr-val min-val max-val]
  MinMax
  (writeMinMax [_this src-vec min-vec min-vec-idx max-vec max-vec-idx]
    (dotimes [src-idx (.getValueCount src-vec)]
      (.read rw src-vec src-idx curr-val)
      (when (or (not (.isSet rw min-val)) (neg? (.compare comparator curr-val min-val)))
        (.read rw src-vec src-idx min-val))

      (when (or (not (.isSet rw max-val)) (pos? (.compare comparator curr-val max-val)))
        (.read rw src-vec src-idx max-val)))

    (.write rw min-vec min-vec-idx min-val)
    (.write rw max-vec max-vec-idx max-val)))

(defn- ->min-max [^FieldVector field-vec]
  (let [minor-type (Types/getMinorTypeForArrowType (.getType (.getField field-vec)))
        ^ReadWrite rw (t/type->rw minor-type)]
    (MinMaxImpl. rw (t/type->comp minor-type) (.newHolder rw) (.newHolder rw) (.newHolder rw))))

(defn- write-min-max [^FieldVector field-vec, ^VectorSchemaRoot metadata-root, idx]
  (let [type-id (byte (t/arrow-type->type-id (.getType (.getField field-vec))))

        ^DenseUnionVector min-vec (.getVector metadata-root "min")
        min-offset (util/write-type-id min-vec idx type-id)

        ^DenseUnionVector max-vec (.getVector metadata-root "max")
        max-offset (util/write-type-id max-vec idx type-id)]
    (when (pos? (.getValueCount field-vec))
      (.writeMinMax ^MinMax (->min-max field-vec)
                    field-vec
                    (.getVectorByType min-vec type-id) min-offset
                    (.getVectorByType max-vec type-id) max-offset))))

(defn write-col-meta [^VectorSchemaRoot metadata-root, ^VectorSchemaRoot live-root, ^String col-name]
  (letfn [(write-vec-meta [^FieldVector field-vec ^String field-name]
            (when (pos? (.getValueCount field-vec))
              (let [idx (.getRowCount metadata-root)
                    type-id (byte (t/arrow-type->type-id (.getType (.getField field-vec))))]

                (doto ^VarCharVector (.getVector metadata-root column-name-field)
                  (.setSafe idx (Text. col-name)))
                (doto ^VarCharVector (.getVector metadata-root field-name-field)
                  (.setSafe idx (Text. field-name)))
                (doto ^TinyIntVector (.getVector metadata-root "type-id")
                  (.setSafe idx type-id))
                (doto ^BigIntVector (.getVector metadata-root "count")
                  (.setSafe idx (.getValueCount field-vec)))

                (write-min-max field-vec metadata-root idx)
                (bloom/write-bloom field-vec metadata-root idx)

                (util/set-vector-schema-root-row-count metadata-root (inc idx)))))]

    (doseq [^FieldVector field-vec (.getFieldVectors live-root)
            :let [field-name (.getName (.getField field-vec))]]
      (if (instance? DenseUnionVector field-vec)
        (doseq [child-vec (.getChildrenFromFields ^DenseUnionVector field-vec)]
          (write-vec-meta child-vec field-name))
        (write-vec-meta field-vec field-name)))))

(defn with-metadata [^IMetadataManager metadata-mgr, ^long chunk-idx, f]
  (.withMetadata metadata-mgr chunk-idx (util/->jfn f)))

(defn with-latest-metadata [^IMetadataManager metadata-mgr, f]
  (if-let [chunk-idx (last (.knownChunks metadata-mgr))]
    (with-metadata metadata-mgr chunk-idx f)
    (CompletableFuture/completedFuture nil)))

(defn- read-max-value [^VectorSchemaRoot metadata-root, ^long idx, ^ValueHolder out-holder]
  (t/read-duv-value (.getVector metadata-root "max") idx out-holder))

(defn latest-tx [^VectorSchemaRoot metadata-root]
  (let [tx-id-idx (expr/metadata-field-idx metadata-root
                                           '_tx-id '_tx-id
                                           (-> (t/primitive-type->arrow-type :bigint)
                                               (t/arrow-type->type-id)))
        tx-id-holder (NullableBigIntHolder.)

        tx-time-idx (expr/metadata-field-idx metadata-root
                                             '_tx-time '_tx-time
                                             (-> (t/primitive-type->arrow-type :timestampmilli)
                                                 (t/arrow-type->type-id)))
        tx-time-holder (NullableTimeStampMilliHolder.)]

    (read-max-value metadata-root tx-id-idx tx-id-holder)
    (read-max-value metadata-root tx-time-idx tx-time-holder)

    (tx/->TransactionInstant (.value tx-id-holder) (Date. (.value tx-time-holder)))))

(defn latest-row-id [^VectorSchemaRoot metadata-root]
  (let [row-id-idx (expr/metadata-field-idx metadata-root '_tx-id '_row-id (-> (t/primitive-type->arrow-type :bigint)
                                                                               (t/arrow-type->type-id)))
        row-id-holder (NullableBigIntHolder.)]
    (read-max-value metadata-root row-id-idx row-id-holder)
    (.value row-id-holder)))

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
                         (doseq [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
                           (write-col-meta metadata-root live-root col-name))

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
