(ns core2.metadata
  (:require core2.buffer-pool
            [core2.select :as sel]
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.buffer_pool.BufferPool
           core2.object_store.ObjectStore
           core2.types.ReadWrite
           java.io.Closeable
           [java.util Comparator Date List SortedSet]
           java.util.concurrent.ConcurrentSkipListSet
           java.util.function.IntPredicate
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector BitVector FieldVector TinyIntVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableTimeStampMilliHolder NullableTinyIntHolder]
           [org.apache.arrow.vector.types Types Types$MinorType]
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def ^org.apache.arrow.vector.types.pojo.Schema metadata-schema
  (Schema. [(t/->field "file" (.getType Types$MinorType/VARCHAR) false)
            (t/->field "column" (.getType Types$MinorType/VARCHAR) false)
            (t/->field "field" (.getType Types$MinorType/VARCHAR) false)
            (t/->field "type-id" (.getType Types$MinorType/TINYINT) false)
            (t/->primitive-dense-union-field "min")
            (t/->primitive-dense-union-field "max")
            (t/->field "count" (.getType Types$MinorType/BIGINT) false)]))

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
  (let [^byte type-id (t/arrow-type->type-id (.getType (.getField field-vec)))

        ^DenseUnionVector min-vec (.getVector metadata-root "min")
        min-offset (util/write-type-id min-vec idx type-id)

        ^DenseUnionVector max-vec (.getVector metadata-root "max")
        max-offset (util/write-type-id max-vec idx type-id)]
    (when (pos? (.getValueCount field-vec))
      (.writeMinMax ^MinMax (->min-max field-vec)
                    field-vec
                    (.getVectorByType min-vec type-id) min-offset
                    (.getVectorByType max-vec type-id) max-offset))))

(defn write-col-meta [^VectorSchemaRoot metadata-root, ^VectorSchemaRoot live-root
                      ^String col-name ^String file-name]
  (letfn [(write-vec-meta [^FieldVector field-vec ^String field-name]
            (when (pos? (.getValueCount field-vec))
              (let [idx (.getRowCount metadata-root)
                    ^byte type-id (t/arrow-type->type-id (.getType (.getField field-vec)))]

                (doto ^VarCharVector (.getVector metadata-root "column")
                  (.setSafe idx (Text. col-name)))
                (doto ^VarCharVector (.getVector metadata-root "field")
                  (.setSafe idx (Text. field-name)))
                (doto ^VarCharVector (.getVector metadata-root "file")
                  (.setSafe idx (Text. file-name)))
                (doto ^TinyIntVector (.getVector metadata-root "type-id")
                  (.setSafe idx type-id))
                (doto ^BigIntVector (.getVector metadata-root "count")
                  (.setSafe idx (.getValueCount field-vec)))

                (write-min-max field-vec metadata-root idx)

                (util/set-vector-schema-root-row-count metadata-root (inc idx)))))]

    (doseq [^FieldVector field-vec (.getFieldVectors live-root)
            :let [field-name (.getName (.getField field-vec))]]
      (if (instance? DenseUnionVector field-vec)
        (doseq [child-vec (.getChildrenFromFields ^DenseUnionVector field-vec)]
          (write-vec-meta child-vec field-name))
        (write-vec-meta field-vec field-name)))))

(definterface IMetadataManager
  (^void registerNewChunk [^java.util.Map roots, ^long chunk-idx])
  (^core2.tx.TransactionInstant latestStoredTx [])
  (^Long latestStoredRowId [])

  (^java.util.List matchingChunks [^String colName
                                   ^org.apache.arrow.vector.holders.ValueHolder lowerBound ^boolean isLowerInclusive
                                   ^org.apache.arrow.vector.holders.ValueHolder upperBound ^boolean isUpperInclusive])

  (^String chunkFileKey [^String chunkKey, ^String fieldName]))

(definterface IMetadataPrivate
  (^int fieldIndex [^org.apache.arrow.vector.VectorSchemaRoot metadataRoot, ^String columnName, ^String fieldName])
  (^int fieldIndex [^org.apache.arrow.vector.VectorSchemaRoot metadataRoot, ^String columnName, ^String fieldName, ^byte typeId])
  (^void readMaxValue [^org.apache.arrow.vector.VectorSchemaRoot metadataRoot,
                       ^String columnName, ^String fieldName,
                       ^org.apache.arrow.vector.holders.ValueHolder outHolder]))

(defn- find-first-set-idx ^long [^BitVector bit-vec]
  (let [vc (.getValueCount bit-vec)]
    (loop [idx 0]
      (cond
        (>= idx vc) -1
        (pos? (.get bit-vec idx)) idx
        :else (recur (inc idx))))))

(defn- with-metadata [^BufferAllocator allocator, ^BufferPool buffer-pool, metadata-key, f]
  (-> (.getBuffer buffer-pool metadata-key)
      (util/then-apply
        (fn [^ArrowBuf metadata-buffer]
          (if metadata-buffer
            (try
              (reduce (completing
                       (fn [_ metadata-root]
                         (reduced (f metadata-root))))
                      nil
                      (util/block-stream metadata-buffer allocator))
              (finally
                (.close metadata-buffer)))

            (f nil))))))

(deftype MetadataManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^BufferPool buffer-pool
                          ^SortedSet known-metadata]
  IMetadataPrivate
  (fieldIndex [this metadata column-name field-name]
    (.fieldIndex this metadata column-name field-name -1))

  (fieldIndex [_ metadata-root column-name field-name type-id]
    (with-open [col-name-holder (t/open-literal-varchar-holder allocator column-name)
                field-name-holder (t/open-literal-varchar-holder allocator field-name)]
      (let [col-vec (.getVector metadata-root "column")
            start-idx (sel/first-index-of col-vec sel/pred= (.holder col-name-holder))]
        (when-not (neg? start-idx)
          (let [end-idx (sel/last-index-of col-vec (.holder col-name-holder) start-idx)]
            (with-open [res-vec (sel/open-result-vec allocator (- end-idx start-idx))]
              (when (pos? type-id)
                (.select (sel/->selector (sel/->vec-pred sel/pred= (doto (NullableTinyIntHolder.)
                                                                     (-> .isSet (set! 1))
                                                                     (-> .value (set! type-id)))))
                         (.getVector metadata-root "type-id") start-idx end-idx res-vec))

              (when (pos? (.select (sel/->selector (sel/->vec-pred sel/pred= (.holder field-name-holder)))
                                   (.getVector metadata-root "field") start-idx end-idx res-vec))
                (+ start-idx (find-first-set-idx res-vec)))))))))

  (readMaxValue [this metadata-root column-name field-name out-holder]
    (let [minor-type (t/holder-minor-type out-holder)
          type-id (t/arrow-type->type-id (.getType minor-type))]
      (if-let [field-idx (.fieldIndex this metadata-root column-name field-name type-id)]
        (let [^DenseUnionVector max-vec (.getVector metadata-root "max")
              ^ReadWrite rw (t/type->rw minor-type)]
          (.read rw (.getVectorByType max-vec type-id) (.getOffset max-vec field-idx) out-holder)
          true)
        false)))

  IMetadataManager
  (registerNewChunk [_ live-roots chunk-idx]
    (let [metadata-buf (with-open [metadata-root (VectorSchemaRoot/create metadata-schema allocator)]
                         (doseq [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
                           (let [obj-key (format "chunk-%08x-%s.arrow" chunk-idx col-name)]
                             (write-col-meta metadata-root live-root col-name obj-key)))

                         (util/root->arrow-ipc-byte-buffer metadata-root :file))
          metadata-obj-key (format "metadata-%08x.arrow" chunk-idx)]

      @(.putObject object-store metadata-obj-key metadata-buf)

      (.add known-metadata metadata-obj-key)))

  (latestStoredTx [this]
    (when-let [metadata-key (last known-metadata)]
      @(with-metadata allocator buffer-pool metadata-key
         (fn [^VectorSchemaRoot metadata-root]
           (let [tx-id-holder (NullableBigIntHolder.)
                 tx-time-holder (NullableTimeStampMilliHolder.)]
             (.readMaxValue this metadata-root "_tx-id" "_tx-id" tx-id-holder)
             (.readMaxValue this metadata-root "_tx-time" "_tx-time" tx-time-holder)
             (tx/->TransactionInstant (.value tx-id-holder)
                                      (Date. (.value tx-time-holder))))))))

  (latestStoredRowId [this]
    (when-let [metadata-key (last known-metadata)]
      @(with-metadata allocator buffer-pool metadata-key
         (fn [^VectorSchemaRoot metadata-root]
           (let [row-id-holder (NullableBigIntHolder.)]
             (.readMaxValue this metadata-root "_tx-id" "_row-id" row-id-holder)
             (.value row-id-holder))))))

  (matchingChunks [this col-name lower-bound lower-inclusive? upper-bound upper-inclusive?]
    (assert (and lower-bound upper-bound (= (type lower-bound) (type upper-bound))))
    (let [minor-type (t/holder-minor-type lower-bound)
          ^ReadWrite rw (t/type->rw minor-type)
          ^Comparator comparator (t/type->comp minor-type)
          type-id (t/arrow-type->type-id (.getType minor-type))

          lower-pred (if lower-inclusive? sel/pred>= sel/pred>)
          upper-pred (if upper-inclusive? sel/pred<= sel/pred<)]

      (->> (vec known-metadata)
           (mapv (fn [metadata-key]
                   (with-metadata allocator buffer-pool metadata-key
                     (fn [^VectorSchemaRoot metadata-root]
                       (let [val-holder (.newHolder rw)]
                         (when-let [field-idx (.fieldIndex this metadata-root col-name col-name type-id)]

                           (letfn [(within-bound [^DenseUnionVector field-vec ^IntPredicate pred bound]
                                     (.read rw (.getVectorByType field-vec type-id) (.getOffset field-vec field-idx) val-holder)
                                     (.test pred (.compare comparator val-holder bound)))]

                             (when (and (or (not (.isSet rw lower-bound))
                                            (within-bound (.getVector metadata-root "max")
                                                          lower-pred lower-bound))

                                        (or (not (.isSet rw upper-bound))
                                            (within-bound (.getVector metadata-root "min")
                                                          upper-pred upper-bound)))
                               metadata-key))))))))
           (into [] (keep deref)))))

  (chunkFileKey [this metadata-key field-name]
    @(with-metadata allocator buffer-pool metadata-key
       (fn [^VectorSchemaRoot metadata-root]
         (when-let [field-idx (.fieldIndex this metadata-root field-name field-name)]
           (str (.getObject (.getVector metadata-root "file") field-idx))))))

  Closeable
  (close [_]
    (.clear known-metadata)))

(defn ->metadata-manager ^core2.metadata.IMetadataManager [^BufferAllocator allocator
                                                           ^ObjectStore object-store
                                                           ^BufferPool buffer-pool]
  (MetadataManager. allocator object-store buffer-pool
                    (ConcurrentSkipListSet. ^List @(.listObjects object-store "metadata-*"))))
