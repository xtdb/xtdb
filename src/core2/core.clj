(ns core2.core
  (:require [clojure.java.io :as io])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream Closeable File]
           [java.nio.channels FileChannel Channels]
           [java.nio.file OpenOption StandardOpenOption]
           [java.nio.charset StandardCharsets]
           [java.util Arrays Base64 Date HashMap Map]
           [java.util.function Function]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.memory.util ByteFunctionHelpers]
           [org.apache.arrow.vector BigIntVector BitVector FieldVector DateMilliVector Float8Vector NullVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.holders NullableBitHolder NullableBigIntHolder NullableFloat8Holder NullableDateMilliHolder NullableVarBinaryHolder NullableVarCharHolder]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowFileWriter ArrowStreamReader ArrowStreamWriter JsonFileReader JsonFileWriter JsonFileWriter$JSONWriteConfig ReadChannel]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType Field FieldType Schema]
           [org.apache.arrow.vector.util Text]))

(defn write-arrow-json-files [^File arrow-dir]
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
    (doseq [^File
            file (->> (.listFiles arrow-dir)
                      (filter #(.endsWith (.getName ^File %) ".arrow")))]
      (with-open [file-ch (FileChannel/open (.toPath file)
                                            (into-array OpenOption #{StandardOpenOption/READ}))
                  file-reader (ArrowFileReader. file-ch allocator)
                  file-writer (JsonFileWriter. (io/file arrow-dir (format "%s.json" (.getName file)))
                                               (.. (JsonFileWriter/config) (pretty true)))]
        (let [root (.getVectorSchemaRoot file-reader)]
          (.start file-writer (.getSchema root) nil)
          (while (.loadNextBatch file-reader)
            (.write file-writer root)))))))

(defprotocol TransactionIngester
  (index-tx [ingester tx docs]))

(def max-block-size 10000)
(def max-blocks-per-chunk 10)

(def ->arrow-type
  {Boolean (.getType Types$MinorType/BIT)
   (Class/forName "[B") (.getType Types$MinorType/VARBINARY)
   Double (.getType Types$MinorType/FLOAT8)
   Long (.getType Types$MinorType/BIGINT)
   String (.getType Types$MinorType/VARCHAR)
   Date (.getType Types$MinorType/DATEMILLI)
   nil (.getType Types$MinorType/NULL)})

(defprotocol PValueVector
  (set-safe! [value-vector idx v])
  (set-null! [value-vector idx]))

(extend-protocol PValueVector
  BigIntVector
  (set-safe! [this idx v] (.setSafe this ^int idx ^long v))
  (set-null! [this idx] (.setNull this ^int idx))

  BitVector
  (set-safe! [this idx v] (.setSafe this ^int idx ^int (if v 1 0)))
  (set-null! [this idx] (.setNull this ^int idx))

  DateMilliVector
  (set-safe! [this idx v] (.setSafe this ^int idx (.getTime ^Date v)))
  (set-null! [this idx] (.setNull this ^int idx))

  Float8Vector
  (set-safe! [this idx v] (.setSafe this ^int idx ^double v))
  (set-null! [this idx] (.setNull this ^int idx))

  NullVector
  (set-safe! [this idx v])
  (set-null! [this idx])

  VarBinaryVector
  (set-safe! [this idx v] (.setSafe this ^int idx ^bytes v))
  (set-null! [this idx] (.setNull this ^int idx))

  VarCharVector
  (set-safe! [this idx v] (.setSafe this ^int idx (Text. (str v))))
  (set-null! [this idx] (.setNull this ^int idx)))

(defprotocol ColumnMetadata
  (update-metadata! [column-metadata field-vector idx])
  (metadata->edn [column-metadata]))

(deftype BitMetadata [^NullableBitHolder min-val, ^NullableBitHolder max-val, ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^BitVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Integer/compare (.get field-vector idx)
                                       (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Integer/compare (.get field-vector idx)
                                       (.value max-val))))
        (.get field-vector idx max-val))))

  (metadata->edn [_]
    {:min (when (pos? (.isSet min-val)) (.value min-val))
     :max (when (pos? (.isSet max-val)) (.value max-val))
     :count cnt})

  Closeable
  (close [_]))

(deftype BigIntMetadata [^NullableBigIntHolder min-val, ^NullableBigIntHolder max-val, ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^BigIntVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Long/compare (.get field-vector idx)
                                    (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Long/compare (.get field-vector idx)
                                    (.value max-val))))
        (.get field-vector idx max-val))))

  (metadata->edn [_]
    {:min (when (pos? (.isSet min-val)) (.value min-val))
     :max (when (pos? (.isSet max-val)) (.value max-val))
     :count cnt})

  Closeable
  (close [_]))

(deftype DateMilliMetadata [^NullableDateMilliHolder min-val, ^NullableDateMilliHolder max-val, ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^DateMilliVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Long/compare (.get field-vector idx)
                                    (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Long/compare (.get field-vector idx)
                                    (.value max-val))))
        (.get field-vector idx max-val))))

  (metadata->edn [_]
    {:min (when (pos? (.isSet min-val)) (Date. (.value min-val)))
     :max (when (pos? (.isSet max-val)) (Date. (.value max-val)))
     :count cnt})

  Closeable
  (close [_]))

(deftype Float8Metadata [^NullableFloat8Holder min-val, ^NullableFloat8Holder max-val, ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^Float8Vector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Double/compare (.get field-vector idx)
                                      (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Double/compare (.get field-vector idx)
                                      (.value max-val))))
        (.get field-vector idx max-val))))

  (metadata->edn [_]
    {:min (when (pos? (.isSet min-val)) (.value min-val))
     :max (when (pos? (.isSet max-val)) (.value max-val))
     :count cnt})

  Closeable
  (close [_]))

(deftype VarBinaryMetadata [^:unsynchronized-mutable ^bytes min-val,
                            ^:unsynchronized-mutable ^bytes max-val,
                            ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^VarBinaryVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (let [value (.getObject field-vector idx)]
        (when (or (nil? min-val) (neg? (Arrays/compareUnsigned value min-val)))
          (set! (.min-val this) value))

        (when (or (nil? max-val) (pos? (Arrays/compareUnsigned value max-val)))
          (set! (.max-val this) value)))))

  (metadata->edn [_]
    {:min (when min-val (.encodeToString (Base64/getEncoder) min-val))
     :max (when max-val (.encodeToString (Base64/getEncoder) max-val))
     :count cnt})

  Closeable
  (close [_]))

(deftype VarCharMetadata [^:unsynchronized-mutable min-val,
                          ^:unsynchronized-mutable max-val,
                          ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^VarCharVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (let [value (str (.getObject field-vector idx))]
        (when (or (nil? min-val) (neg? (compare value min-val)))
          (set! (.min-val this) value))

        (when (or (nil? max-val) (pos? (compare value max-val)))
          (set! (.max-val this) value)))))

  (metadata->edn [_]
    {:min min-val
     :max max-val
     :count cnt})

  Closeable
  (close [_]))

(defn ->metadata [^ArrowType arrow-type]
  (condp = (Types/getMinorTypeForArrowType arrow-type)
    Types$MinorType/BIT (->BitMetadata (NullableBitHolder.) (NullableBitHolder.) 0)
    Types$MinorType/BIGINT (->BigIntMetadata (NullableBigIntHolder.) (NullableBigIntHolder.) 0)
    Types$MinorType/DATEMILLI (->DateMilliMetadata (NullableDateMilliHolder.) (NullableDateMilliHolder.) 0)
    Types$MinorType/FLOAT8 (->Float8Metadata (NullableFloat8Holder.) (NullableFloat8Holder.) 0)
    Types$MinorType/VARBINARY (->VarBinaryMetadata nil nil 0)
    Types$MinorType/VARCHAR (->VarCharMetadata nil nil 0)
    (throw (UnsupportedOperationException.))))

(declare close-writers!)

(defn- ->field ^Field [^String field-name ^ArrowType arrow-type nullable & children]
  (Field. field-name
          (FieldType. nullable arrow-type nil nil)
          children))

(def ^:private tx-arrow-schema
  (Schema. [(->field "tx-ops" (.getType Types$MinorType/DENSEUNION) true
                     (->field "put" (.getType Types$MinorType/STRUCT) false
                              (->field "document" (.getType Types$MinorType/DENSEUNION) false))
                     (->field "delete" (.getType Types$MinorType/STRUCT) true))]))

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
                                                        [k (Field/nullable (name k) (->arrow-type (type v)))])
                                                      (into (sorted-map)))
                                      field-k (format "%08x" (hash doc-fields))
                                      ^Field doc-field (apply ->field field-k (.getType Types$MinorType/STRUCT) true (vals doc-fields))
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
                                      (set-safe! field-vec per-struct-offset v)
                                      (set-null! field-vec per-struct-offset)))

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

(def ^:private ^Field row-id-field
  (->field "_row-id" (.getType Types$MinorType/BIGINT) false))

(def ^:private ^Field tx-time-field
  (->field "_tx-time" (.getType Types$MinorType/DATEMILLI) false))

(def ^:private ^Field tx-id-field
  (->field "_tx-id" (.getType Types$MinorType/BIGINT) false))

(deftype LiveColumn [^VectorSchemaRoot content-root, ^ArrowFileWriter file-writer
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
                             content-root (VectorSchemaRoot/create (Schema. [row-id-field field]) allocator)
                             file-ch (FileChannel/open (.toPath (io/file arrow-dir (format "chunk-%08x-%s.arrow" chunk-idx (field->file-name field))))
                                                       (into-array OpenOption #{StandardOpenOption/CREATE
                                                                                StandardOpenOption/WRITE
                                                                                StandardOpenOption/TRUNCATE_EXISTING}))]
                         (LiveColumn. content-root
                                      (doto (ArrowFileWriter. content-root nil file-ch)
                                        (.start))
                                      (->metadata (.getType row-id-field))
                                      (->metadata (.getType field))))))]
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
                               ^BigIntVector row-id-vec (.getVector content-root row-id-field)
                               row-id (+ next-row-id tx-op-idx)]

                           (.copyFromSafe field-vec per-struct-offset value-count kv-vec)
                           (.setSafe row-id-vec value-count row-id)
                           (.setRowCount content-root (inc value-count))

                           (update-metadata! (.row-id-metadata live-column) row-id-vec value-count)
                           (update-metadata! (.field-metadata live-column) field-vec value-count)))

                       (letfn [(set-tx-field! [^Field field field-value]
                                 (let [^LiveColumn live-column (.computeIfAbsent field->live-column field ->column)
                                       ^VectorSchemaRoot content-root (.content-root live-column)
                                       value-count (.getRowCount content-root)
                                       field-vec (.getVector content-root field)
                                       ^BigIntVector row-id-vec (.getVector content-root row-id-field)
                                       row-id (+ next-row-id tx-op-idx)]

                                   (set-safe! field-vec value-count field-value)
                                   (.setSafe row-id-vec value-count row-id)
                                   (.setRowCount content-root (inc value-count))

                                   (update-metadata! (.row-id-metadata live-column) row-id-vec value-count)
                                   (update-metadata! (.field-metadata live-column) field-vec value-count)))]

                         (set-tx-field! tx-time-field tx-time)
                         (set-tx-field! tx-id-field tx-id))))))

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

(defn- close-writers! [^Ingester ingester chunk-idx]
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

      (let [metadata-file (io/file (.arrow-dir ingester)
                                   (format "metadata-%08x.edn" chunk-idx))]
        ;; TODO: next: metadata -> arrow
        (spit metadata-file
              (pr-str (into {}
                            (for [[^Field field, ^LiveColumn live-column] field->live-column]
                              [(format "chunk-%08x-%s.arrow" chunk-idx (field->file-name field))
                               {"_row-id" (metadata->edn (.row-id-metadata live-column))
                                (.getName field) (metadata->edn (.field-metadata live-column))}])))))
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
