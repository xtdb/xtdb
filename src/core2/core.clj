(ns core2.core
  (:require [clojure.java.io :as io])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream Closeable File]
           [java.nio.channels FileChannel Channels]
           [java.nio.file OpenOption StandardOpenOption]
           [java.util Arrays Date HashMap Map]
           [java.util.function Function]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector BitVector FieldVector DateMilliVector Float8Vector NullVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
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

(defprotocol SetSafe
  (set-safe! [_ idx v])
  (set-null! [_ idx]))

(extend-protocol SetSafe
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

(deftype LiveColumn [^VectorSchemaRoot content-root, ^ArrowFileWriter file-writer, !metadata])

(defn- field->file-name [^Field field]
  (format "%s-%s-%08x"
          (.getName field)
          (str (Types/getMinorTypeForArrowType (.getType field)))
          (hash field)))

(defn- update-metadata [metadata field value]
  (let [less-than? (fn [x y]
                     (neg? (cond
                             (bytes? x) (Arrays/compareUnsigned ^bytes x ^bytes y)
                             (instance? Text x) (compare (str x) (str y))
                             :else (compare x y))))]
    (-> metadata
        (update field (fn [field-metadata]
                        (-> field-metadata
                            (update :min
                                    (fn [min-value]
                                      (if min-value
                                        (if (less-than? min-value value)
                                          min-value
                                          value)
                                        value)))
                            (update :max
                                    (fn [max-value]
                                      (if max-value
                                        (if (less-than? max-value value)
                                          value
                                          max-value)
                                        value)))
                            (update :count inc)))))))

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
                                      (atom {row-id-field {:count 0}, field {:count 0}})))))]
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
                       (doseq [^ValueVector kv-vec (.getChildrenFromFields (.getStruct document-vec struct-type-id))]
                         (let [field (.getField kv-vec)
                               ^LiveColumn live-column (.computeIfAbsent field->live-column field ->column)
                               ^VectorSchemaRoot content-root (.content-root live-column)
                               !metadata (.!metadata live-column)
                               value-count (.getRowCount content-root)
                               ^FieldVector field-vec (.getVector content-root field)
                               ^BigIntVector row-id-vec (.getVector content-root row-id-field)
                               row-id (+ next-row-id tx-op-idx)]

                           (.copyFromSafe field-vec per-struct-offset value-count kv-vec)
                           (.setSafe row-id-vec value-count row-id)
                           (.setRowCount content-root (inc value-count))

                           (swap! (.!metadata live-column) update-metadata row-id-field row-id)
                           (swap! (.!metadata live-column) update-metadata field (.getObject field-vec value-count))))

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

                                   (swap! (.!metadata live-column) update-metadata row-id-field row-id)
                                   (swap! (.!metadata live-column) update-metadata field field-value)))]
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
    (doseq [^LiveColumn live-column (vals field->live-column)
            :let [^VectorSchemaRoot root (.content-root live-column)
                  ^ArrowFileWriter file-writer (.file-writer live-column)]]
      (try
        (when (pos? (.getRowCount root))
          (.writeBatch file-writer)

          (doseq [^FieldVector field-vector (.getFieldVectors root)]
            (.reset field-vector))

          (.setRowCount root 0))
        (finally
          (try
            (.close file-writer)
            (.close root)
            (catch Exception e
              (.printStackTrace e))))))

    (let [metadata-file (io/file (.arrow-dir ingester)
                                 (format "metadata-%08x.edn" chunk-idx))]
      ;; TODO: next: metadata -> arrow
      (spit metadata-file
            (pr-str (into {}
                          (for [[^Field field, ^LiveColumn live-column] field->live-column
                                :let [metadata @(.!metadata live-column)]]
                            [(format "chunk-%08x-%s.arrow" chunk-idx (field->file-name field))
                             (into {} (for [[^Field field, field-metadata] metadata]
                                        [(.getName field) field-metadata]))])))))

    (.clear field->live-column)))

(defn ->ingester ^core2.core.Ingester [^BufferAllocator allocator ^File arrow-dir]
  (Ingester. allocator
             arrow-dir
             (HashMap.)
             0
             0))
