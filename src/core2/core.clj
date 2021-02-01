(ns core2.core
  (:require [clojure.java.io :as io]
            [core2.metadata :as meta]
            [core2.types :as t])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream Closeable File]
           [java.nio.channels Channels FileChannel]
           [java.nio.file OpenOption StandardOpenOption]
           [java.util Date HashMap List Map]
           java.util.function.Function
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector FieldVector ValueVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamReader ArrowStreamWriter]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.util.Text
           core2.metadata.IColumnMetadata))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol TransactionIngester
  (index-tx [ingester tx docs]))

(declare close-writers!)

(def ^:private tx-arrow-schema
  (Schema. [(t/->field "tx-ops" (.getType Types$MinorType/DENSEUNION) false
                       (t/->field "put" (.getType Types$MinorType/STRUCT) false
                                  (t/->field "document" (.getType Types$MinorType/DENSEUNION) false))
                       (t/->field "delete" (.getType Types$MinorType/STRUCT) true))]))

(defn- add-element-to-union! [^DenseUnionVector duv type-id ^long parent-offset child-offset]
  (while (< (.getValueCapacity duv) (inc parent-offset))
    (.reAlloc duv))

  (.setTypeId duv parent-offset type-id)
  (.setInt (.getOffsetBuffer duv)
           (* DenseUnionVector/OFFSET_WIDTH parent-offset)
           child-offset))

(defn submit-tx ^bytes [tx-ops ^RootAllocator allocator]
  (with-open [root (VectorSchemaRoot/create tx-arrow-schema allocator)]
    (let [^DenseUnionVector tx-op-vec (.getVector root "tx-ops")

          union-type-ids (into {} (map vector
                                       (->> (.getChildren (.getField tx-op-vec))
                                            (map #(keyword (.getName ^Field %))))
                                       (range)))]

      (->> (map-indexed vector tx-ops)
           (reduce (fn [acc [op-idx {:keys [op] :as tx-op}]]
                     (let [^int per-op-offset (get-in acc [:per-op-offsets op] 0)
                           ^int op-type-id (get union-type-ids op)]

                       (add-element-to-union! tx-op-vec op-type-id op-idx per-op-offset)

                       (case op
                         :put (let [{:keys [doc]} tx-op

                                    ^StructVector put-vec (.getStruct tx-op-vec op-type-id)
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
                                      per-struct-offset (long (get-in acc [:put :per-struct-offsets doc-field] 0))]

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
                   ^Map field->live-column
                   ^long max-block-size
                   ^long max-blocks-per-chunk
                   ^:unsynchronized-mutable ^long chunk-idx
                   ^:unsynchronized-mutable ^long next-row-id]

  TransactionIngester
  (index-tx [this {:keys [tx-time tx-id]} tx-bytes]
    (let [->column (reify Function
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

    ;; TODO better metric here?
    ;; row-id? bytes? tx-id?
    (when (>= (->> (vals field->live-column)
                   (map (fn [^LiveColumn live-column]
                          (count (.getRecordBlocks ^ArrowFileWriter (.file-writer live-column)))))
                   (apply max)
                   (long))
              max-blocks-per-chunk)
      (close-writers! this chunk-idx)

      (set! (.chunk-idx this) next-row-id)))

  Closeable
  (close [this]
    (close-writers! this chunk-idx)))

(defn- write-metadata! [^Ingester ingester, ^long chunk-idx]
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
    (with-open [metadata-file-ch (open-write-file-ch (io/file (.arrow-dir ingester) (format "metadata-%08x.arrow" chunk-idx)))
                metadata-fw (ArrowFileWriter. metadata-root nil metadata-file-ch)]
      (.start metadata-fw)
      (.writeBatch metadata-fw))))

(defn- close-writers! [^Ingester ingester, ^long chunk-idx]
  (let [^Map field->live-column (.field->live-column ingester)]
    (try
      (doseq [^LiveColumn live-column (vals field->live-column)
              :let [^VectorSchemaRoot content-root (.content-root live-column)]]
        (when (pos? (.getRowCount content-root))
          (write-live-column live-column)))

      (write-metadata! ingester chunk-idx)

      (finally
        (doseq [^LiveColumn live-column (vals field->live-column)]
          (.close live-column))

        (.clear field->live-column)))))

(defn ->ingester
  (^core2.core.Ingester [^BufferAllocator allocator ^File arrow-dir]
   (->ingester allocator arrow-dir {}))
  (^core2.core.Ingester [^BufferAllocator allocator ^File arrow-dir {:keys [max-block-size max-blocks-per-chunk]
                                                                     :or {max-block-size 10000
                                                                          max-blocks-per-chunk 10}}]
   (Ingester. allocator
              arrow-dir
              (HashMap.)
              max-block-size
              max-blocks-per-chunk
              0
              0)))
