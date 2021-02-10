(ns core2.indexer
  (:require [core2.metadata :as meta]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.object_store.ObjectStore
           java.io.Closeable
           [java.nio.file Files Path StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.util Date Map]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot VectorLoader VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamReader]
           [org.apache.arrow.vector.types.pojo Field Schema]))

(set! *unchecked-math* :warn-on-boxed)

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])

(definterface TransactionIndexer
  (^core2.indexer.TransactionInstant indexTx [^core2.indexer.TransactionInstant tx ^java.nio.ByteBuffer txOps]))

(definterface IndexerPrivate
  (^void closeCols [])
  (^void finishChunk []))

(defn- copy-safe! [^VectorSchemaRoot content-root ^DenseUnionVector src-vec src-idx row-id]
  (let [[^BigIntVector row-id-vec, ^DenseUnionVector field-vec] (.getFieldVectors content-root)
        value-count (.getRowCount content-root)
        type-id (.getTypeId src-vec src-idx)
        offset (util/write-type-id field-vec (.getValueCount field-vec) type-id)]

    (.setSafe row-id-vec value-count ^int row-id)

    (.copyFromSafe (.getVectorByType field-vec type-id)
                   (.getOffset src-vec src-idx)
                   offset
                   (.getVectorByType src-vec type-id))

    (.setRowCount content-root (inc value-count))))

(defn- write-column! [^VectorSchemaRoot root, ^BufferAllocator allocator, ^Path tmp-file ^long max-rows-per-block]
  (with-open [write-root (VectorSchemaRoot/create (.getSchema root) allocator)
              file-ch (util/->file-channel tmp-file #{StandardOpenOption/CREATE
                                                      StandardOpenOption/WRITE
                                                      StandardOpenOption/TRUNCATE_EXISTING})
              fw (ArrowFileWriter. write-root nil file-ch)]
    (.start fw)

    (let [row-count (.getRowCount root)
          loader (VectorLoader. write-root)]

      ;; TODO: between-col block alignment? at the moment, each block will have `max-rows-per-block` rows,
      ;; but values for the same row-id may not be in the same block for different cols.
      (doseq [^long block-idx (range (cond-> (quot row-count max-rows-per-block)
                                       (pos? ^long (mod row-count max-rows-per-block)) inc))
              :let [start-idx (* max-rows-per-block block-idx)
                    end-idx (min (+ start-idx max-rows-per-block) row-count)
                    len (- end-idx start-idx)
                    sliced-root (.slice root start-idx len)]]
        (try
          (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
            (.load loader arb)
            (.writeBatch fw))

          (finally
            ;; slice returns the same VSR object if it's asked to slice the whole thing
            ;; we don't want to close it in that case, because that will mutate the live-root.
            (when-not (identical? sliced-root root)
              (.close sliced-root))))))
    (.end fw)))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-time-field
  (t/->primitive-dense-union-field "_tx-time" #{:timestampmilli}))

(def ^:private timestampmilli-type-id
  (-> (t/primitive-type->arrow-type :timestampmilli)
      (t/arrow-type->type-id)))

(defn ->tx-time-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^Date tx-time]
  (doto ^DenseUnionVector (.createVector tx-time-field allocator)
    (.setValueCount 1)
    (util/write-type-id 0 timestampmilli-type-id)
    (-> (.getTimeStampMilliVector timestampmilli-type-id)
        (.setSafe 0 (.getTime tx-time)))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-id-field
  (t/->primitive-dense-union-field "_tx-id" #{:bigint}))

(def ^:private bigint-type-id
  (-> (t/primitive-type->arrow-type :bigint)
      (t/arrow-type->type-id)))

(defn ->tx-id-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^long tx-id]
  (doto ^DenseUnionVector (.createVector tx-id-field allocator)
    (.setValueCount 1)
    (util/write-type-id 0 bigint-type-id)
    (-> (.getBigIntVector bigint-type-id)
        (.setSafe 0 tx-id))))

(defn- ->live-root [field-name allocator]
  (VectorSchemaRoot/create (Schema. [t/row-id-field (t/->primitive-dense-union-field field-name)]) allocator))

(deftype Indexer [^BufferAllocator allocator
                  ^Path arrow-dir
                  ^ObjectStore object-store
                  ^Map live-roots
                  ^long max-rows-per-chunk
                  ^long max-rows-per-block
                  ^:volatile-mutable ^long chunk-idx
                  ^:volatile-mutable ^long next-row-id]

  TransactionIndexer
  (indexTx [this tx-instant tx-ops]
    (with-open [tx-ops-ch (util/->seekable-byte-channel tx-ops)
                sr (ArrowStreamReader. tx-ops-ch allocator)
                tx-root (.getVectorSchemaRoot sr)
                ^DenseUnionVector tx-id-vec (->tx-id-vec allocator (.tx-id tx-instant))
                ^DenseUnionVector tx-time-vec (->tx-time-vec allocator (.tx-time tx-instant))]

      (.loadNextBatch sr)

      (doseq [^Field field (concat [tx-time-field tx-id-field]
                                   (-> (.getSchema tx-root)
                                       (.findField "tx-ops") .getChildren
                                       (Schema/findField "put") .getChildren
                                       (Schema/findField "document") .getChildren))]
        (.computeIfAbsent live-roots (.getName field)
                          (util/->jfn
                           (fn [field-name]
                             (->live-root field-name allocator)))))

      (let [^DenseUnionVector tx-ops-vec (.getVector tx-root "tx-ops")
            op-type-ids (object-array (mapv (fn [^Field field]
                                              (keyword (.getName field)))
                                            (.getChildren (.getField tx-ops-vec))))]
        (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
          (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
                op-vec (.getStruct tx-ops-vec op-type-id)
                per-op-offset (.getOffset tx-ops-vec tx-op-idx)]
            (case (aget op-type-ids op-type-id)
              :put (let [^StructVector document-vec (.getChild op-vec "document" StructVector)
                         row-id (+ next-row-id per-op-offset)]
                     (doseq [^DenseUnionVector value-vec (.getChildrenFromFields document-vec)
                             :when (not (neg? (.getTypeId value-vec per-op-offset)))]

                       (copy-safe! (get live-roots (.getName (.getField value-vec)))
                                   value-vec per-op-offset row-id))

                     (copy-safe! (get live-roots (.getName (.getField tx-time-vec)))
                                 tx-time-vec 0 row-id)

                     (copy-safe! (get live-roots (.getName (.getField tx-id-vec)))
                                 tx-id-vec 0 row-id)))))

        (set! (.next-row-id this) (+ next-row-id (.getValueCount tx-ops-vec)))))

    ;; TODO better metric here?
    ;; row-id? bytes? tx-id?
    (when (>= (->> (vals live-roots)
                   (map (fn [^VectorSchemaRoot root]
                          (.getRowCount root)))
                   (apply max)
                   (long))
              max-rows-per-chunk)
      (.finishChunk this))

    tx-instant)

  IndexerPrivate
  (closeCols [_this]
    (doseq [^VectorSchemaRoot live-root (vals live-roots)]
      (util/try-close live-root))

    (.clear live-roots))

  (finishChunk [this]
    (when-not (.isEmpty live-roots)
      (with-open [metadata-root (VectorSchemaRoot/create meta/metadata-schema allocator)]
        (let [futs (vec
                    (for [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
                      (let [tmp-file (.resolve arrow-dir (format "chunk-%08x-%s.arrow" chunk-idx col-name))
                            file-name (str (.getFileName tmp-file))]
                        (write-column! live-root allocator tmp-file max-rows-per-block)

                        (let [fut (.putObject object-store file-name tmp-file)]
                          (meta/write-col-meta metadata-root live-root col-name file-name)

                          (-> fut
                              (util/then-apply
                               (fn [_]
                                 (Files/delete tmp-file))))))))

              metadata-file (.resolve arrow-dir (format "metadata-%08x.arrow" chunk-idx))]

          (with-open [metadata-file-ch (util/->file-channel metadata-file #{StandardOpenOption/CREATE
                                                                            StandardOpenOption/WRITE
                                                                            StandardOpenOption/TRUNCATE_EXISTING})
                      metadata-fw (ArrowFileWriter. metadata-root nil metadata-file-ch)]
            (.start metadata-fw)
            (.writeBatch metadata-fw)
            (.end metadata-fw))

          @(-> (CompletableFuture/allOf (into-array CompletableFuture futs))
               (util/then-compose
                (fn [_]
                  (.putObject object-store (str (.getFileName metadata-file)) metadata-file))))

          (.closeCols this)
          (Files/delete metadata-file)
          (set! (.chunk-idx this) next-row-id)))))

  Closeable
  (close [this]
    (.closeCols this)
    (util/delete-dir arrow-dir)))

(defn ->indexer
  (^core2.indexer.Indexer [^BufferAllocator allocator
                         ^ObjectStore object-store
                         ^Long latest-row-id]
   (->indexer allocator object-store latest-row-id {}))

  (^core2.indexer.Indexer [^BufferAllocator allocator
                         ^ObjectStore object-store
                         ^Long latest-row-id
                         {:keys [max-rows-per-chunk max-rows-per-block]
                          :or {max-rows-per-chunk 10000
                               max-rows-per-block 1000}}]
   (let [next-row-id (if latest-row-id
                       (inc (long latest-row-id))
                       0)]
     (Indexer. allocator
               (Files/createTempDirectory "core2-indexer" (make-array FileAttribute 0))
               object-store
               (ConcurrentHashMap.)
               max-rows-per-chunk
               max-rows-per-block
               next-row-id
               next-row-id))))
