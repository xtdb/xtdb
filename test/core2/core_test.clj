(ns core2.core-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.instant :as inst]
            [clojure.string :as str])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream Closeable File]
           [java.nio.channels FileChannel Channels]
           [java.nio.file OpenOption StandardOpenOption]
           [java.util Date HashMap Map]
           [java.util.function Function]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector BitVector FieldVector DateMilliVector Float8Vector NullVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowFileWriter ArrowStreamReader ArrowStreamWriter JsonFileReader JsonFileWriter JsonFileWriter$JSONWriteConfig ReadChannel]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType Field FieldType Schema]
           [org.apache.arrow.vector.util Text]))

(def device-info-csv-resource
  (io/resource "devices_small_device_info.csv"))

(def !info-docs
  (delay
    (when device-info-csv-resource
      (with-open [rdr (io/reader device-info-csv-resource)]
        (vec (for [device-info (line-seq rdr)
                   :let [[device-id api-version manufacturer model os-name] (str/split device-info #",")]]
               {:_id (str "device-info-" device-id)
                :api-version api-version
                :manufacturer manufacturer
                :model model
                :os-name os-name}))))))

(def readings-csv-resource
  (io/resource "devices_small_readings.csv"))

(defn with-readings-docs [f]
  (when readings-csv-resource
    (with-open [rdr (io/reader readings-csv-resource)]
      (f (for [[time device-id battery-level battery-status
                battery-temperature bssid
                cpu-avg-1min cpu-avg-5min cpu-avg-15min
                mem-free mem-used rssi ssid]
               (csv/read-csv rdr)]
           {:time (inst/read-instant-date
                   (-> time
                       (str/replace " " "T")
                       (str/replace #"-(\d\d)$" ".000-$1:00")))
            :_id (str "reading-" device-id)
            :battery-level (Double/parseDouble battery-level)
            :battery-status battery-status
            :battery-temperature (Double/parseDouble battery-temperature)
            :bssid bssid
            :cpu-avg-1min (Double/parseDouble cpu-avg-1min)
            :cpu-avg-5min (Double/parseDouble cpu-avg-5min)
            :cpu-avg-15min (Double/parseDouble cpu-avg-15min)
            :mem-free (Double/parseDouble mem-free)
            :mem-used (Double/parseDouble mem-used)
            :rssi (Double/parseDouble rssi)
            :ssid ssid})))))

(comment
  (defonce foo-rows
    (with-readings-docs
      (fn [rows]
        (doall (take 10 rows))))))

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

;;; ingest
;; TODO 4. writing metadata - minmax, bloom at chunk/file and block/record-batch
;; TODO 11. figure out last tx-id/row-id from latest chunk and resume ingest on start.
;; TODO 12. object store protocol, store chunks and metadata. File implementation.
;; TODO 13. log protocol. File implementation.

;;; query
;; TODO 7. reading any blocks - select battery_level from db (simple code-level query)
;; TODO 7b. fetch metadata and find further chunks based on metadata.
;; TODO 8. reading live blocks
;; TODO 8a. reading chunks already written to disk
;; TODO 8b. reading blocks already written to disk
;; TODO 8c. reading current block not yet written to disk
;; TODO 8d. VSR committed read slice

;;; future
;; TODO 3d. dealing with schema that changes throughout an ingest (promotable unions, including nulls)
;; TODO 5. dictionaries
;; TODO 6. consider eviction
;; TODO 1b. writer?
;; TODO 15. handle deletes

;; directions?
;; 1. e2e? submit-tx + some code-level queries
;;    transactions, evictions, timelines, etc.
;; 2. quickly into JMH, experimentation

;; once we've sealed a _chunk_ (/ block?), throw away in-memory? mmap or load
;; abstract over this - we don't need to know whether it's mmap'd or in-memory

;; reading a block before it's sealed?
;; theory: if we're only appending, we should be ok?

;; two different cases:
;; live chunk, reading sealed block (maybe) written to disk
;; live chunk, reading unsealed block in memory

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

;; submit-tx - function from tx-ops to Arrow

(defn- ^Field ->field [^String field-name ^ArrowType arrow-type nullable & children]
  (Field. field-name
          (FieldType. nullable arrow-type nil nil)
          children))

'{:tx-time 'date-milli
  :tx-id 'uint8
  :tx-ops {:put [{:documents []
                  :start-vts [...]
                  :end-vts [...]}]
           :delete []}}

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

;; submit-tx :: ops -> Arrow bytes
;; Kafka mock - Arrow * tx-time/tx-id
;; ingester :: Arrow bytes * tx-time * tx-id -> indices

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

#_
(def foo-tx-bytes
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
    (with-readings-docs
      (fn [readings]
        (let [info-docs @!info-docs]
          (vec (for [tx-batch (->> (concat (interleave info-docs
                                                       (take (count info-docs) readings))
                                           (drop (count info-docs) readings))
                                   (partition-all 1000))]
                 (submit-tx (for [doc tx-batch]
                              {:op :put, :doc doc})
                            allocator))))))))

(def ^:private ^Field row-id-field
  (->field "_row-id" (.getType Types$MinorType/BIGINT) false))

(def ^:private ^Field tx-time-field
  (->field "_tx-time" (.getType Types$MinorType/DATEMILLI) false))

(def ^:private ^Field tx-id-field
  (->field "_tx-id" (.getType Types$MinorType/BIGINT) false))

(deftype LiveColumn [^VectorSchemaRoot content-root, ^ArrowFileWriter file-writer, !metadata])

(def ^:private metadata-schema
  (Schema. [(->)]))

'{:file-name ["name.arrow" "age.arrow"]
  :field-name ["name" "age"]
  :field-metadata [{:min 4 :max 2300 :count 100}
                   {:min "Aaron" :max "Zach" :count 1000}]
  :row-id-metadata [{:min 4 :max 2300 :count 100}
                    {:min 10 :max 30 :count 1000}]}

'{:metadata [{:chunk_00000_age {:_row-id {:min 4 :max 2300 :count 100}
                                :age {:min 10 :max 30 :count 1000}}
              :chunk_00000_name {:_row-id {:min 4 :max 2300 :count 100}
                                 :name {:min "Aaron" :max "Zach" :count 1000}}}]}

'{:chunk_00000_age [{:_row-id {:min 4 :max 2300 :count 100}
                     :age {:min 10 :max 30 :count 1000}}]
  :chunk_00000_name [{:_row-id {:min 4 :max 2300 :count 100}
                      :name {:min "Aaron" :max "Zach" :count 1000}}]}

(defn field->file-name [^Field field]
  (format "%s-%s-%08x"
          (.getName field)
          (str (Types/getMinorTypeForArrowType (.getType field)))
          (hash field)))

(do
  ;; aim: one metadata file per chunk with:
  ;; - count per column
  ;; - min-max per column

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
                                 field-vec (.getVector content-root field)
                                 ^BigIntVector row-id-vec (.getVector content-root row-id-field)]

                             (swap! !metadata (fn [metadata]
                                                (-> metadata
                                                    (update-in [row-id-field :count] inc)
                                                    (update-in [field :count] inc))))

                             (.copyFromSafe field-vec per-struct-offset value-count kv-vec)
                             (.setSafe row-id-vec value-count (+ next-row-id tx-op-idx))
                             (.setRowCount content-root (inc value-count))))

                         (letfn [(set-tx-field! [^Field field field-val]
                                   (let [^LiveColumn live-column (.computeIfAbsent field->live-column field ->column)
                                         ^VectorSchemaRoot content-root (.content-root live-column)
                                         value-count (.getRowCount content-root)
                                         field-vec (.getVector content-root field)
                                         ^BigIntVector row-id-vec (.getVector content-root row-id-field)]

                                     (set-safe! field-vec value-count field-val)
                                     (.setSafe row-id-vec value-count (+ next-row-id tx-op-idx))
                                     (.setRowCount content-root (inc value-count))))]
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

      ;; TODO remove
      '{:chunk_00000_age [{:_row-id {:min 4 :max 2300 :count 100}
                           :age {:min 10 :max 30 :count 1000}}]
        :chunk_00000_name [{:_row-id {:min 4 :max 2300 :count 100}
                            :name {:min "Aaron" :max "Zach" :count 1000}}]}

      (let [metadata-file (io/file (.arrow-dir ingester)
                                   (format "metadata-%08x.edn" chunk-idx))]
        (spit metadata-file
              (pr-str (into {}
                            (for [[^Field field, ^LiveColumn live-column] field->live-column
                                  :let [metadata @(.!metadata live-column)]]
                              [(format "chunk-%08x-%s.arrow" chunk-idx (field->file-name field))
                               (into {} (for [[^Field field, field-metadata] metadata]
                                          [(.getName field) field-metadata]))])))))

      (.clear field->live-column)))

  #_
  (let [arrow-dir (doto (io/file "/tmp/arrow")
                    .mkdirs)]
    (with-open [allocator (RootAllocator. Long/MAX_VALUE)
                ingester (Ingester. allocator
                                    arrow-dir
                                    (HashMap.)
                                    0
                                    0)]

      (doseq [[tx-id tx-bytes] (->> (map-indexed vector foo-tx-bytes)
                                    (take 15))]
        (index-tx ingester {:tx-id tx-id, :tx-time (Date.)} tx-bytes)))

    (write-arrow-json-files arrow-dir)))

(comment
  ;; converting Arrow to JSON
  )
