(ns core2.ingest
  (:require [clojure.java.io :as io]
            [core2.metadata :as meta]
            core2.object-store
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.metadata.IColumnMetadata
           core2.object_store.ObjectStore
           [java.io ByteArrayInputStream Closeable File]
           java.nio.ByteBuffer
           java.nio.channels.FileChannel
           [java.nio.file Files OpenOption StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.util Date HashMap List Map]
           java.util.concurrent.CompletableFuture
           java.util.function.Function
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector FieldVector ValueVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamReader]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.util.Text))

;; TODO rename to indexer

(set! *unchecked-math* :warn-on-boxed)

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])

(definterface TransactionIngester
  (^core2.ingest.TransactionInstant indexTx [^core2.ingest.TransactionInstant tx ^java.nio.ByteBuffer txOps])
  (^void finishChunk []))

(declare close-writers! write-metadata!)

(deftype LiveColumn [^VectorSchemaRoot content-root, ^File file, ^ArrowFileWriter file-writer, ^List field-metadata]
  Closeable
  (close [_]
    (doseq [^Closeable metadata field-metadata]
      (.close metadata))
    (.close file-writer)
    (.close content-root)))

(defn- field->file-name [^Field field]
  (format "%s-%s-%08x"
          (.getName field)
          (str (Types/getMinorTypeForArrowType (.getType field)))
          (hash (str field))))

(defn- open-write-file-ch ^java.nio.channels.FileChannel [^File file]
  (FileChannel/open (.toPath file)
                    (into-array OpenOption #{StandardOpenOption/CREATE
                                             StandardOpenOption/WRITE
                                             StandardOpenOption/TRUNCATE_EXISTING})))

(defn- write-live-column [^LiveColumn live-column]
  (let [^VectorSchemaRoot content-root (.content-root live-column)]
    (.writeBatch ^ArrowFileWriter (.file-writer live-column))

    (dotimes [n (count (.getFieldVectors content-root))]
      (let [field-vector (.getVector content-root n)]
        (.updateMetadata ^IColumnMetadata (get (.field-metadata live-column) n) field-vector)
        (.reset field-vector)))

    (.setRowCount content-root 0)))

(defn- ->column-schema ^org.apache.arrow.vector.types.pojo.Schema [^Field field]
  (Schema. [t/row-id-field field]))

(deftype Ingester [^BufferAllocator allocator
                   ^File arrow-dir
                   ^ObjectStore object-store
                   ^Map field->live-column
                   ^long max-block-size
                   ^long max-blocks-per-chunk
                   ^:volatile-mutable ^long chunk-idx
                   ^:volatile-mutable ^long next-row-id]

  TransactionIngester
  (indexTx [this tx-instant tx-ops]
    (let [tx-time (.tx-time tx-instant)
          tx-id (.tx-id tx-instant)
          ->column (reify Function
                     (apply [_ field]
                       (let [^Field field field
                             schema (->column-schema field)
                             field-metadata (vec (for [^Field field (.getFields schema)]
                                                   (meta/->metadata (.getType field))))
                             content-root (VectorSchemaRoot/create schema allocator)
                             file (io/file arrow-dir (format "chunk-%08x-%s.arrow" chunk-idx (field->file-name field)))
                             file-ch (open-write-file-ch file)]
                         (LiveColumn. content-root
                                      file
                                      (doto (ArrowFileWriter. content-root nil file-ch)
                                        (.start))
                                      field-metadata))))]
      (with-open [tx-ops-ch (util/->seekable-byte-channel tx-ops)
                  sr (doto (ArrowStreamReader. tx-ops-ch allocator)
                       (.loadNextBatch))
                  tx-root (.getVectorSchemaRoot sr)]

        (let [^DenseUnionVector tx-ops-vec (.getVector tx-root "tx-ops")
              op-type-ids (object-array (mapv (fn [^Field field]
                                                (keyword (.getName field)))
                                              (.getChildren (.getField tx-ops-vec))))]
          (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
            (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
                  op-vec (.getStruct tx-ops-vec op-type-id)
                  per-op-offset (.getOffset tx-ops-vec tx-op-idx)]
              (case (aget op-type-ids op-type-id)
                :put (let [^DenseUnionVector document-vec (.addOrGet op-vec "document" (t/->field-type (.getType Types$MinorType/DENSEUNION) false) DenseUnionVector)
                           struct-type-id (.getTypeId document-vec per-op-offset)
                           per-struct-offset (.getOffset document-vec per-op-offset)]
                       ;; TODO duplication here between KV and tx metadata? `copyFromSafe` vs `set-safe!`
                       ;; we also considered whether we were able to 'just' copy the whole vector here
                       (doseq [^ValueVector kv-vec (.getChildrenFromFields (.getStruct document-vec struct-type-id))]
                         (let [field (.getField kv-vec)
                               ^LiveColumn live-column (.computeIfAbsent field->live-column field ->column)
                               ^VectorSchemaRoot content-root (.content-root live-column)
                               value-count (.getRowCount content-root)
                               ^BigIntVector row-id-vec (.getVector content-root t/row-id-field)
                               ^FieldVector field-vec (.getVector content-root field)
                               row-id (+ next-row-id tx-op-idx)]

                           (.setSafe row-id-vec value-count row-id)
                           (.copyFromSafe field-vec per-struct-offset value-count kv-vec)
                           (.setRowCount content-root (inc value-count))))

                       (letfn [(set-tx-field! [^Field field field-value]
                                 (let [^LiveColumn live-column (.computeIfAbsent field->live-column field ->column)
                                       ^VectorSchemaRoot content-root (.content-root live-column)
                                       value-count (.getRowCount content-root)
                                       ^BigIntVector row-id-vec (.getVector content-root t/row-id-field)
                                       field-vec (.getVector content-root field)
                                       row-id (+ next-row-id tx-op-idx)]

                                   (.setSafe row-id-vec value-count row-id)
                                   (t/set-safe! field-vec value-count field-value)
                                   (.setRowCount content-root (inc value-count))))]

                         (set-tx-field! t/tx-time-field tx-time)
                         (set-tx-field! t/tx-id-field tx-id))))))

          (set! (.next-row-id this) (+ next-row-id (.getValueCount tx-ops-vec))))

        (doseq [^LiveColumn live-column (vals field->live-column)
                :let [^VectorSchemaRoot content-root (.content-root live-column)]]
          (when (>= (.getRowCount content-root) max-block-size)
            (write-live-column live-column)))))

    tx-instant

    ;; TODO better metric here?
    ;; row-id? bytes? tx-id?
    (when (>= (->> (vals field->live-column)
                   (map (fn [^LiveColumn live-column]
                          (count (.getRecordBlocks ^ArrowFileWriter (.file-writer live-column)))))
                   (apply max)
                   (long))
              max-blocks-per-chunk)
      (.finishChunk this)))

  (finishChunk [this]
    (when-not (empty? field->live-column)
      (doseq [^LiveColumn live-column (vals field->live-column)
              :let [^VectorSchemaRoot content-root (.content-root live-column)]]
        (when (pos? (.getRowCount content-root))
          (write-live-column live-column)))

      (let [files (vec (for [^LiveColumn live-column (vals field->live-column)
                             :let [^File file (.file live-column)]
                             :when (.exists file)]
                         file))
            metadata-file (io/file (.arrow-dir this) (format "metadata-%08x.arrow" chunk-idx))]

        (write-metadata! this metadata-file)
        (close-writers! this)

        (.join (CompletableFuture/allOf
                (into-array CompletableFuture
                            (for [^File file files]
                              (.putObject object-store (.getName file) (.toPath file))))))

        (doseq [^File file files]
          (.delete file))

        (.join (.putObject object-store (.getName metadata-file) (.toPath metadata-file)))

        (.delete metadata-file))

      (set! (.chunk-idx this) next-row-id)))

  Closeable
  (close [this]
    (close-writers! this)
    (util/delete-dir arrow-dir)))

(defn- close-writers! [^Ingester ingester]
  (let [^Map field->live-column (.field->live-column ingester)]
    (doseq [^LiveColumn live-column (vals field->live-column)]
      (.close live-column))

    (.clear field->live-column)))

(defn- write-metadata! [^Ingester ingester, ^File metadata-file]
  (with-open [metadata-root (VectorSchemaRoot/create meta/metadata-schema (.allocator ingester))]
    (reduce
     (fn [^long idx [^Field field, ^LiveColumn live-column]]
       (let [content-root ^VectorSchemaRoot (.content-root live-column)
             field-metadata (.field-metadata live-column)
             file-name (Text. (.getName ^File (.file live-column)))
             column-name (Text. (.getName field))]
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
     (sort-by (comp str key) (.field->live-column ingester)))

    (.syncSchema metadata-root)

    (with-open [metadata-file-ch (open-write-file-ch metadata-file)
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
                (.toFile (Files/createTempDirectory "core2-ingester" (make-array FileAttribute 0)))
                object-store
                (HashMap.)
                max-block-size
                max-blocks-per-chunk
                next-row-id
                next-row-id))))
