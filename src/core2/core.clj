(ns core2.core
  (:require [clojure.java.io :as io]
            [core2.metadata :as meta]
            [core2.types :as t])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream Closeable File]
           [java.nio.channels Channels FileChannel]
           [java.nio.file OpenOption StandardOpenOption]
           [java.util Date HashMap Map]
           java.util.function.Function
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector FieldVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamReader ArrowStreamWriter]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field FieldType Schema]))

(defprotocol TransactionIngester
  (index-tx [ingester tx docs]))

(def max-block-size 10000)
(def max-blocks-per-chunk 10)

(declare close-writers!)

(def ^:private tx-arrow-schema
  (Schema. [(t/->field "tx-ops" (.getType Types$MinorType/DENSEUNION) true
                       (t/->field "put" (.getType Types$MinorType/STRUCT) false
                                  (t/->field "document" (.getType Types$MinorType/DENSEUNION) false))
                       (t/->field "delete" (.getType Types$MinorType/STRUCT) true))]))

(defn- add-element-to-union! [^DenseUnionVector duv type-id parent-offset child-offset]
  (while (< (.getValueCapacity duv) (inc parent-offset))
    (.reAlloc duv))

  (.setTypeId duv parent-offset type-id)
  (.setInt (.getOffsetBuffer duv)
           (* DenseUnionVector/OFFSET_WIDTH parent-offset)
           child-offset))

(defn submit-tx ^VectorSchemaRoot [tx-ops ^RootAllocator allocator]
  (with-open [root (VectorSchemaRoot/create tx-arrow-schema allocator)]
    (let [^DenseUnionVector tx-op-vec (.getVector root "tx-ops")

          union-type-ids (into {} (map vector
                                       (->> (.getChildren (.getField tx-op-vec))
                                            (map #(keyword (.getName ^Field %))))
                                       (range)))]

      (->> (map-indexed vector tx-ops)
           (reduce (fn [acc [op-idx {:keys [op] :as tx-op}]]
                     (let [^int per-op-offset (get-in acc [:per-op-offsets op] 0)]

                       (add-element-to-union! tx-op-vec (get union-type-ids op) op-idx per-op-offset)

                       (case op
                         :put (let [{:keys [doc]} tx-op
                                    put-type-id (get union-type-ids :put)

                                    ^StructVector put-vec (.getStruct tx-op-vec put-type-id)
                                    ^DenseUnionVector put-doc-vec (.getChild put-vec "document" DenseUnionVector)]

                                (.setIndexDefined put-vec per-op-offset)

                                ;; TODO we could/should key this just by the field name, and have a promotable union in the value.
                                ;; but, for now, it's keyed by both field name and type.
                                (let [doc-fields (->> (for [[k v] (sort-by key doc)]
                                                        [k (t/->field (name k) (t/->arrow-type (type v)) true)])
                                                      (into (sorted-map)))
                                      field-k (format "%08x" (hash doc-fields))
                                      ^Field doc-field (apply t/->field field-k (.getType Types$MinorType/STRUCT) true (vals doc-fields))
                                      field-type-id (or (->> (map-indexed vector (keys (get-in acc [:put :per-struct-offsets])))
                                                             (some (fn [[idx ^Field field]]
                                                                     (when (= doc-field field)
                                                                       idx))))
                                                        (.registerNewTypeId put-doc-vec doc-field))
                                      struct-vec (.getStruct put-doc-vec field-type-id)
                                      per-struct-offset (get-in acc [:put :per-struct-offsets doc-field] 0)]

                                  (.setIndexDefined struct-vec per-struct-offset)

                                  (add-element-to-union! put-doc-vec field-type-id per-op-offset per-struct-offset)

                                  (doseq [[k v] doc
                                          :let [^Field field (get doc-fields k)
                                                field-vec (.addOrGet struct-vec (name k) (.getFieldType field) ValueVector)]]
                                    (if (some? v)
                                      (t/set-safe! field-vec per-struct-offset v)
                                      (t/set-null! field-vec per-struct-offset)))

                                  (-> acc
                                      (assoc-in [:per-op-offsets :put] (inc per-op-offset))
                                      (assoc-in [:put :per-struct-offsets doc-field] (inc per-struct-offset))))))))
                   {}))

      (.setRowCount root (count tx-ops))
      (.syncSchema root)

      (with-open [baos (ByteArrayOutputStream.)
                  sw (ArrowStreamWriter. root nil (Channels/newChannel baos))]
        (doto sw
          (.start)
          (.writeBatch)
          (.end))
        (.toByteArray baos)))))

(deftype LiveColumn [^VectorSchemaRoot content-root, ^File file, ^ArrowFileWriter file-writer
                     ^Closeable row-id-metadata, ^Closeable field-metadata]
  Closeable
  (close [_]
    (.close row-id-metadata)
    (.close field-metadata)
    (.close file-writer)
    (.close content-root)))

(defn- field->file-name [^Field field]
  (format "%s-%s-%08x"
          (.getName field)
          (str (Types/getMinorTypeForArrowType (.getType field)))
          (hash field)))

(defn- open-write-file-ch [^File file]
  (FileChannel/open (.toPath file)
                    (into-array OpenOption #{StandardOpenOption/CREATE
                                             StandardOpenOption/WRITE
                                             StandardOpenOption/TRUNCATE_EXISTING})))

(deftype Ingester [^BufferAllocator allocator
                   ^File arrow-dir
                   ^Map field->live-column
                   ^:unsynchronized-mutable ^long chunk-idx
                   ^:unsynchronized-mutable ^long next-row-id]

  TransactionIngester
  (index-tx [this {:keys [tx-time tx-id]} tx-bytes]
    (let [->column (reify Function
                     (apply [_ field]
                       (let [^Field field field
                             content-root (VectorSchemaRoot/create (Schema. [t/row-id-field field]) allocator)
                             file (io/file arrow-dir (format "chunk-%08x-%s.arrow" chunk-idx (field->file-name field)))
                             file-ch (open-write-file-ch file)]
                         (LiveColumn. content-root
                                      file
                                      (doto (ArrowFileWriter. content-root nil file-ch)
                                        (.start))
                                      (meta/->metadata (.getType t/row-id-field))
                                      (meta/->metadata (.getType field))))))]
      (with-open [bais (ByteArrayInputStream. tx-bytes)
                  sr (doto (ArrowStreamReader. bais allocator)
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
                :put (let [^DenseUnionVector document-vec (.addOrGet op-vec "document" (FieldType. false (.getType Types$MinorType/DENSEUNION) nil nil) DenseUnionVector)
                           struct-type-id (.getTypeId document-vec per-op-offset)
                           per-struct-offset (.getOffset document-vec per-op-offset)]
                       ;; TODO duplication here between KV and tx metadata? `copyFromSafe` vs `set-safe!`
                       ;; we also considered whether we were able to 'just' copy the whole vector here
                       (doseq [^ValueVector kv-vec (.getChildrenFromFields (.getStruct document-vec struct-type-id))]
                         (let [field (.getField kv-vec)
                               ^LiveColumn live-column (.computeIfAbsent field->live-column field ->column)
                               ^VectorSchemaRoot content-root (.content-root live-column)
                               value-count (.getRowCount content-root)
                               ^FieldVector field-vec (.getVector content-root field)
                               ^BigIntVector row-id-vec (.getVector content-root t/row-id-field)
                               row-id (+ next-row-id tx-op-idx)]

                           (.copyFromSafe field-vec per-struct-offset value-count kv-vec)
                           (.setSafe row-id-vec value-count row-id)
                           (.setRowCount content-root (inc value-count))

                           (meta/update-metadata! (.row-id-metadata live-column) row-id-vec value-count)
                           (meta/update-metadata! (.field-metadata live-column) field-vec value-count)))

                       (letfn [(set-tx-field! [^Field field field-value]
                                 (let [^LiveColumn live-column (.computeIfAbsent field->live-column field ->column)
                                       ^VectorSchemaRoot content-root (.content-root live-column)
                                       value-count (.getRowCount content-root)
                                       field-vec (.getVector content-root field)
                                       ^BigIntVector row-id-vec (.getVector content-root t/row-id-field)
                                       row-id (+ next-row-id tx-op-idx)]

                                   (t/set-safe! field-vec value-count field-value)
                                   (.setSafe row-id-vec value-count row-id)
                                   (.setRowCount content-root (inc value-count))

                                   (meta/update-metadata! (.row-id-metadata live-column) row-id-vec value-count)
                                   (meta/update-metadata! (.field-metadata live-column) field-vec value-count)))]

                         (set-tx-field! t/tx-time-field tx-time)
                         (set-tx-field! t/tx-id-field tx-id))))))

          (set! (.next-row-id this) (+ next-row-id (.getValueCount tx-ops-vec))))

        (doseq [^LiveColumn live-column (vals field->live-column)
                :let [^VectorSchemaRoot content-root (.content-root live-column)
                      ^ArrowFileWriter file-writer (.file-writer live-column)]]
          (when (>= (.getRowCount content-root) max-block-size)
            (.writeBatch file-writer)

            (doseq [^FieldVector field-vector (.getFieldVectors content-root)]
              (.reset field-vector))

            (.setRowCount content-root 0)))))

    ;; TODO better metric here?
    ;; row-id? bytes? tx-id?
    (when (>= (->> (vals field->live-column)
                   (map (fn [^LiveColumn live-column]
                          (count (.getRecordBlocks ^ArrowFileWriter (.file-writer live-column)))))
                   (apply max))
              max-blocks-per-chunk)
      (close-writers! this chunk-idx)

      (set! (.chunk-idx this) next-row-id)))

  Closeable
  (close [this]
    (close-writers! this chunk-idx)))

(defn- write-metadata! [^Ingester ingester, chunk-idx]
  (let [^Map field->live-column (.field->live-column ingester)
        schema (Schema. (for [[^Field field, ^LiveColumn live-column] field->live-column]
                          (t/->field (.getName ^File (.file live-column))
                                     (.getType Types$MinorType/STRUCT)
                                     false
                                     (meta/->metadata-field t/row-id-field)
                                     (meta/->metadata-field field))))
        metadata-file-ch (open-write-file-ch (io/file (.arrow-dir ingester) (format "metadata-%08x.arrow" chunk-idx)))]

    (with-open [metadata-root (VectorSchemaRoot/create schema (.allocator ingester))
                metadata-fw (doto (ArrowFileWriter. metadata-root nil metadata-file-ch)
                              (.start))]

      (doseq [[^Field field, ^LiveColumn live-column] field->live-column]
        (let [^StructVector file-vec (.getVector metadata-root (.getName ^File (.file live-column)))]
          (meta/write-metadata! (.field-metadata live-column)
                                (.getChild file-vec (.getName field))
                                0)
          (meta/write-metadata! (.row-id-metadata live-column)
                                (.getChild file-vec (.getName t/row-id-field))
                                0)))
      (.setRowCount metadata-root 1)
      (.writeBatch metadata-fw))))

(defn- close-writers! [^Ingester ingester, chunk-idx]
  (let [^Map field->live-column (.field->live-column ingester)]
    (try
      (doseq [^LiveColumn live-column (vals field->live-column)
              :let [^VectorSchemaRoot root (.content-root live-column)
                    ^ArrowFileWriter file-writer (.file-writer live-column)]]
        (when (pos? (.getRowCount root))
          (.writeBatch file-writer)

          (doseq [^FieldVector field-vector (.getFieldVectors root)]
            (.reset field-vector))

          (.setRowCount root 0)))

      (write-metadata! ingester chunk-idx)

      (finally
        (doseq [^LiveColumn live-column (vals field->live-column)]
          (.close live-column))

        (.clear field->live-column)))))

(defn ->ingester ^core2.core.Ingester [^BufferAllocator allocator ^File arrow-dir]
  (Ingester. allocator
             arrow-dir
             (HashMap.)
             0
             0))
