(ns core2.ingest
  (:require [core2.metadata :as meta]
            [core2.types :as t]
            [core2.util :as util]
            [core2.object-store :as os])
  (:import core2.metadata.IColumnMetadata
           core2.object_store.ObjectStore
           [java.io Closeable]
           java.nio.file.attribute.FileAttribute
           [java.nio.file Files Path StandardOpenOption]
           [java.util Date LinkedList List Map]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]
           java.util.function.Function
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector FieldVector ValueVector VarCharVector VectorSchemaRoot VectorLoader VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamReader]
           [org.apache.arrow.vector.ipc.message ArrowRecordBatch]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.util.Text))

;; TODO rename to indexer

(set! *unchecked-math* :warn-on-boxed)

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])

(definterface TransactionIngester
  (^core2.ingest.TransactionInstant indexTx [^core2.ingest.TransactionInstant tx ^java.nio.ByteBuffer txOps]))

(definterface Finish
  (^void finishBlock [])
  (^void finishChunk []))

(declare close-cols! write-metadata!)

(definterface ILiveColumn
  (^void promoteField [^org.apache.arrow.vector.types.pojo.Field field])
  (^void copySafe [^org.apache.arrow.vector.complex.DenseUnionVector src-vec ^int src-idx ^int row-id])
  (^void setSafe [v ^int row-id]) ; TODO boxing v. holder?
  (^void writeBlock [])
  (^void writeColumn []))

(deftype LiveColumn [^VectorSchemaRoot content-root,
                     ^List record-batches,
                     ^List field-metadata,
                     ^Path file]
  ILiveColumn
  (promoteField [_this field]
    ;; TODO: check, and promote
    )

  (copySafe [_this src-vec src-idx row-id]
    (let [[^BigIntVector row-id-vec, ^DenseUnionVector field-vec] (.getFieldVectors content-root)
          value-count (.getRowCount content-root)
          type-id (.getTypeId src-vec src-idx)
          offset (util/write-type-id field-vec (.getValueCount field-vec) type-id)]
      (.setSafe row-id-vec value-count row-id)
      (.copyFromSafe (.getVectorByType field-vec type-id)
                     (.getOffset src-vec src-idx)
                     offset
                     (.getVectorByType src-vec type-id))
      (.setRowCount content-root (inc value-count))))

  ;; atm this is just used for tx-time/tx-id
  (setSafe [_this v row-id]
    (let [[^BigIntVector row-id-vec, ^DenseUnionVector field-vec] (.getFieldVectors content-root)
          value-count (.getRowCount content-root)]
      (.setSafe row-id-vec value-count row-id)
      (t/set-safe! field-vec value-count v)
      (.setRowCount content-root (inc value-count))))

  (writeBlock [_this]
    (.add record-batches (.getRecordBatch (VectorUnloader. content-root)))

    (dotimes [n (count (.getFieldVectors content-root))]
      (let [field-vector (.getVector content-root n)]
        (.updateMetadata ^IColumnMetadata (get field-metadata n) field-vector)
        (.clear field-vector)))

    (.setRowCount content-root 0))

  (writeColumn [_this]
    (with-open [file-ch (util/->file-channel file #{StandardOpenOption/CREATE
                                                    StandardOpenOption/WRITE
                                                    StandardOpenOption/TRUNCATE_EXISTING})
                fw (ArrowFileWriter. content-root nil file-ch)]
      (.start fw)
      (let [loader (VectorLoader. content-root)]

        (doseq [^ArrowRecordBatch batch record-batches]
          (.load loader batch)
          (.writeBatch fw)
          (util/try-close batch)))
      (.end fw)))

  Closeable
  (close [_]
    (doseq [^ArrowRecordBatch record-batch record-batches]
      (util/try-close record-batch))

    (util/try-close content-root)))

(defn- ->live-column [^Field field out-path allocator]
  (let [fields [t/row-id-field field]
        field-metadata (vec (for [^Field field fields]
                              (meta/->metadata (.getType field))))
        content-root (VectorSchemaRoot/create (Schema. fields) allocator)]
    (LiveColumn. content-root (LinkedList.) field-metadata out-path)))

(deftype Ingester [^BufferAllocator allocator
                   ^Path arrow-dir
                   ^ObjectStore object-store
                   ^Map field-name->live-column
                   ^long max-block-size
                   ^long max-blocks-per-chunk
                   ^:volatile-mutable ^long chunk-idx
                   ^:volatile-mutable ^long next-row-id]

  TransactionIngester
  (indexTx [this tx-instant tx-ops]
    (let [tx-time (.tx-time tx-instant)
          tx-id (.tx-id tx-instant)]
      (with-open [tx-ops-ch (util/->seekable-byte-channel tx-ops)
                  sr (ArrowStreamReader. tx-ops-ch allocator)
                  tx-root (.getVectorSchemaRoot sr)]

        (.loadNextBatch sr)

        (doseq [^Field field (concat [t/tx-time-field t/tx-id-field]
                                     (-> (.getSchema tx-root)
                                         (.findField "tx-ops") .getChildren
                                         (Schema/findField "put") .getChildren
                                         (Schema/findField "document") .getChildren))]
          (.compute field-name->live-column (.getName field)
                    (util/->jbifn
                      (fn [field-name live-col]
                        (if-let [^LiveColumn live-col live-col]
                          (.promoteField live-col field)
                          (->live-column field
                                         (.resolve arrow-dir
                                                   (format "chunk-%08x-%s.arrow" chunk-idx field-name))
                                         allocator))))))

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
                               :when (pos? (.getTypeId value-vec per-op-offset))]

                         (doto ^LiveColumn (get field-name->live-column (.getName (.getField value-vec)))
                           (.copySafe value-vec per-op-offset row-id)))

                       (doto ^LiveColumn (get field-name->live-column (.getName t/tx-time-field))
                         (.setSafe tx-time row-id))

                       (doto ^LiveColumn (get field-name->live-column (.getName t/tx-id-field))
                         (.setSafe tx-id row-id))))))

          (set! (.next-row-id this) (+ next-row-id (.getValueCount tx-ops-vec))))))

    ;; TODO better metric here?
    ;; row-id? bytes? tx-id?
    (when (>= (->> (vals field-name->live-column)
                   (map (fn [^LiveColumn live-column]
                          (count (.record-batches live-column))))
                   (apply max)
                   (long))
              max-blocks-per-chunk)
      (.finishChunk this))

    tx-instant)

  Finish
  (finishBlock [_this]
    (doseq [^LiveColumn live-column (vals field-name->live-column)
            :let [^VectorSchemaRoot content-root (.content-root live-column)]]
      (when (pos? (.getRowCount content-root))
        (.writeBlock live-column))))

  (finishChunk [this]
    (when-not (.isEmpty field-name->live-column)
      (.finishBlock this)

      (let [files (vec (for [^LiveColumn live-column (vals field-name->live-column)
                             :when (not-empty (.record-batches live-column))]
                         (do
                           (.writeColumn live-column)
                           (.file live-column))))
            metadata-file (.resolve ^Path (.arrow-dir this) (format "metadata-%08x.arrow" chunk-idx))]

        (write-metadata! this metadata-file)
        (close-cols! this)

        (.join (CompletableFuture/allOf
                (into-array CompletableFuture
                            (for [^Path file files]
                              (.putObject object-store (str (.getFileName file)) file)))))

        (doseq [^Path file files]
          (Files/delete file))

        (.join (.putObject object-store (str (.getFileName metadata-file)) metadata-file))

        (Files/delete metadata-file))

      (set! (.chunk-idx this) next-row-id)))

  Closeable
  (close [this]
    (close-cols! this)
    (util/delete-dir arrow-dir)))

(defn- close-cols! [^Ingester ingester]
  (let [^Map field->live-column (.field-name->live-column ingester)]
    (doseq [^LiveColumn live-column (vals field->live-column)]
      (util/try-close live-column))

    (.clear field->live-column)))

(defn- write-metadata! [^Ingester ingester, ^Path metadata-file]
  (with-open [metadata-root (VectorSchemaRoot/create meta/metadata-schema (.allocator ingester))]
    (reduce
     (fn [^long idx [^String field-name, ^LiveColumn live-column]]
       (let [content-root ^VectorSchemaRoot (.content-root live-column)
             field-metadata (.field-metadata live-column)
             file-name (Text. (str (.getFileName ^Path (.file live-column))))
             column-name (Text. field-name)]
         (dotimes [n (count (.getFieldVectors content-root))]
           (let [field-vector (.getVector content-root n)
                 idx (+ idx n)]
             (.setSafe ^VarCharVector (.getVector metadata-root "file") idx file-name)
             (.setSafe ^VarCharVector (.getVector metadata-root "column") idx column-name)
             (.setSafe ^VarCharVector (.getVector metadata-root "field") idx (Text. (.getName (.getField field-vector))))
             (.writeMetadata ^IColumnMetadata (get field-metadata n) metadata-root idx)))
         (let [idx (+ idx (count (.getFieldVectors content-root)))]
           (.setRowCount metadata-root idx)
           idx)))
     0
     (sort-by (comp str key) (.field-name->live-column ingester)))

    (.syncSchema metadata-root)

    (with-open [metadata-file-ch (util/->file-channel metadata-file #{StandardOpenOption/CREATE
                                                                      StandardOpenOption/WRITE
                                                                      StandardOpenOption/TRUNCATE_EXISTING})
                metadata-fw (ArrowFileWriter. metadata-root nil metadata-file-ch)]
      (.start metadata-fw)
      (.writeBatch metadata-fw))))

(defn ->ingester
  (^core2.ingest.Ingester [^BufferAllocator allocator
                           ^ObjectStore object-store
                           ^Long latest-row-id]
   (->ingester allocator object-store latest-row-id {}))

  (^core2.ingest.Ingester [^BufferAllocator allocator
                           ^ObjectStore object-store
                           ^Long latest-row-id
                           {:keys [max-block-size max-blocks-per-chunk]
                            :or {max-block-size 10000
                                 max-blocks-per-chunk 10}}]
   (let [next-row-id (if latest-row-id
                       (inc (long latest-row-id))
                       0)]
     (Ingester. allocator
                (Files/createTempDirectory "core2-ingester" (make-array FileAttribute 0))
                object-store
                (ConcurrentHashMap.)
                max-block-size
                max-blocks-per-chunk
                next-row-id
                next-row-id))))
