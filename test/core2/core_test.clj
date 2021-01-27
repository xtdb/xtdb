(ns core2.core-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.instant :as inst]
            [clojure.string :as str])
  (:import [clojure.lang Keyword]
           [java.io ByteArrayInputStream ByteArrayOutputStream Closeable File]
           [java.nio.channels FileChannel Channels]
           [java.nio.file OpenOption StandardOpenOption]
           [java.util Date HashMap Map]
           [java.util.function Function]
           [org.apache.arrow.memory ArrowBuf BufferAllocator RootAllocator]
           [org.apache.arrow.vector FieldVector DateMilliVector Float8Vector ValueVector VarCharVector VectorLoader VectorSchemaRoot UInt8Vector]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector MapVector StructVector]
           [org.apache.arrow.vector.complex.impl VarCharWriterImpl DenseUnionWriter]
           [org.apache.arrow.vector.holders DateMilliHolder Float8Holder VarCharHolder UInt8Holder]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowFileWriter ArrowStreamReader ArrowStreamWriter JsonFileReader JsonFileWriter JsonFileWriter$JSONWriteConfig ReadChannel]
           [org.apache.arrow.vector.ipc.message ArrowBlock MessageSerializer]
           [org.apache.arrow.vector.types Types Types$MinorType UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$List ArrowType$Map ArrowType$Union Field FieldType Schema]
           [org.apache.arrow.vector.util Text]))

(def device-info-csv-resource
  (io/resource "devices_small_device_info.csv"))

(def !info-docs
  (delay
    (when device-info-csv-resource
      (with-open [rdr (io/reader device-info-csv-resource)]
        (vec (for [device-info (line-seq rdr)
                   :let [[device-id api-version manufacturer model os-name] (str/split device-info #",")]]
               {:crux.db/id (keyword "device-info" device-id)
                :device-info/api-version api-version
                :device-info/manufacturer manufacturer
                :device-info/model model
                :device-info/os-name os-name}))))))

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
            :crux.db/id device-id
            :battery-level (Double/parseDouble battery-level)
            :battery-status (keyword battery-status)
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

;; Writer API?

;; DONE 1. let's just get a chunk on disk, forget 'live' blocks - many blocks to a chunk, many chunks
;; TODO 1b. writer?
;; TODO 1c. row-ids
;; DONE 2. more than one block per chunk - 'seal' a block, starting a new live block
;; DONE 3. dynamic documents - atm we've hardcoded cols and types
;; TODO 3b. union types, or keying the block by the type of the value too
;; DONE 3c. intermingle devices with readings
;; TODO 3d. dealing with schema that changes throughout an ingest (promotable unions)
;; TODO 4. metadata - minmax, bloom at chunk/file and block/record-batch
;; TODO 5. dictionaries
;; TODO 6. consider eviction
;; TODO 7. reading any blocks - select battery_level from db (simple code-level query)
;; DONE 8. reading live blocks
;; DONE 8a. reading chunks already written to disk
;; DONE 8b. reading blocks already written to disk
;; DONE 8c. reading current block not yet written to disk

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
  (index-tx [ingester docs]))

(def max-block-size 10000)
(def max-blocks-per-chunk 10)

(def ->arrow-type
  {Double (.getType Types$MinorType/FLOAT8)
   String (.getType Types$MinorType/VARCHAR)
   Keyword (.getType Types$MinorType/VARCHAR)
   Date (.getType Types$MinorType/DATEMILLI)
   nil (.getType Types$MinorType/NULL)})

(defprotocol SetSafe
  (set-safe! [_ idx v]))

(extend-protocol SetSafe
  Float8Vector
  (set-safe! [this idx v]
    (.setSafe this ^int idx ^double v))

  VarCharVector
  (set-safe! [this idx v]
    (.setSafe this ^int idx (Text. (str v))))

  DateMilliVector
  (set-safe! [this idx v]
    (.setSafe this ^int idx (.getTime ^Date v))))

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

                                (let [doc-fields (->> (for [[k v] (sort-by key doc)]
                                                        [k (Field/nullable (pr-str k) (->arrow-type (type v)))])
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
                                          :let [^Field field (get doc-fields k)]]
                                    (set-safe! (.addOrGet struct-vec (pr-str k) (.getFieldType field) ValueVector)
                                               per-struct-offset
                                               v))

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
(defonce foo-tx-bytes
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
    (with-readings-docs
      (fn [readings]
        (submit-tx (for [doc (interleave (take 10 @!info-docs)
                                         (take 10 readings))]
                     {:op :put
                      :doc doc})
                   allocator)))))

(do
  (deftype Ingester [^BufferAllocator allocator
                     ^File arrow-dir
                     ^Map roots ; Field -> VectorSchemaRoot
                     ^Map file-writers ; Field -> ArrowFileWriter
                     ^:unsynchronized-mutable ^long chunk-idx]

    TransactionIngester
    ;; aim: content-indices
    (index-tx [this tx-bytes]
      (with-open [bais (ByteArrayInputStream. tx-bytes)
                  sr (doto (ArrowStreamReader. bais allocator)
                       (.loadNextBatch))
                  root (.getVectorSchemaRoot sr)]

        (let [^DenseUnionVector tx-ops-vec (.getVector root "tx-ops")
              op-type-ids (->> (.getChildren (.getField tx-ops-vec))
                               (into {} (map-indexed (fn [idx ^Field field]
                                                       [idx (keyword (.getName field))])) ))]

          (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
            (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
                  op-vec (.getStruct tx-ops-vec op-type-id)
                  per-op-offset (.getOffset tx-ops-vec tx-op-idx)]
              (case (get op-type-ids op-type-id)
                :put (let [^DenseUnionVector
                           document-vec (.addOrGet op-vec "document" (FieldType. false (.getType Types$MinorType/DENSEUNION) nil nil) DenseUnionVector)
                           struct-type-id (.getTypeId document-vec per-op-offset)
                           per-struct-offset (.getOffset document-vec per-op-offset)]
                       ;; TODO change to doseq
                       (doseq [^ValueVector kv-vec (.getChildrenFromFields (.getStruct document-vec struct-type-id))]
                         (let [^Field field (.getField kv-vec)
                               row-id-field (->field ":crux/row-id" (.getType Types$MinorType/UINT8) false) ; TODO re-use
                               ^VectorSchemaRoot
                               target-root (.computeIfAbsent roots field
                                                             (reify Function
                                                               (apply [_ _]
                                                                 (VectorSchemaRoot/create (Schema. [field row-id-field]) allocator))))
                               value-count (.getRowCount target-root)
                               field-vec (.getVector target-root field)
                               ^UInt8Vector row-id-vec (.getVector target-root row-id-field)]
                           (.copyFromSafe field-vec per-struct-offset value-count kv-vec)
                           (.setSafe row-id-vec value-count tx-op-idx)
                           (.setRowCount target-root (inc value-count)))))))))

        ;; TODO build up files over multiple transactions
        (doseq [^VectorSchemaRoot target-root (vals roots)]
          (with-open [file-ch (FileChannel/open (.toPath (io/file arrow-dir (format "chunk-%08x-%s.arrow" chunk-idx (name (read-string (.getName ^Field (first (.getFields (.getSchema target-root)))))))))
                                                (into-array OpenOption #{StandardOpenOption/CREATE
                                                                         StandardOpenOption/WRITE
                                                                         StandardOpenOption/TRUNCATE_EXISTING}))
                      fw (ArrowFileWriter. target-root nil file-ch)]
            (doto fw
              (.start)
              (.writeBatch)
              (.end))))
        #_
        (doseq [doc docs]
          (let [schema (Schema. (for [[k v] (sort-by key doc)]
                                  (Field/nullable (str k) (->arrow-type (type v)))))

                ^VectorSchemaRoot
                root (.computeIfAbsent roots
                                       schema
                                       (reify Function
                                         (apply [_ _]
                                           (VectorSchemaRoot/create schema allocator))))

                ^ArrowFileWriter
                live-file-writer (.computeIfAbsent file-writers
                                                   schema
                                                   (reify Function
                                                     (apply [_ _]
                                                       (let [file-ch (FileChannel/open (.toPath (io/file arrow-dir (format "chunk-%08x-%08x.arrow" chunk-idx (hash schema))))
                                                                                       (into-array OpenOption #{StandardOpenOption/CREATE
                                                                                                                StandardOpenOption/WRITE
                                                                                                                StandardOpenOption/TRUNCATE_EXISTING}))]
                                                         (doto (ArrowFileWriter. root nil file-ch)
                                                           (.start))))))

                row-count (.getRowCount root)]

            (doseq [[k v] doc]
              (set-safe! (.getVector root (str k)) row-count v))

            (.setRowCount root (inc row-count))

            (when (>= (.getRowCount root) max-block-size)
              (.writeBatch live-file-writer)

              (doseq [^FieldVector field-vector (.getFieldVectors root)]
                (.reset field-vector))

              (.setRowCount root 0)))))

      ;; TODO better metric here?
      #_
      (when (>= (->> (vals file-writers)
                     (transduce (map (fn [^ArrowFileWriter file-writer]
                                       (count (.getRecordBlocks file-writer))))
                                +))
                max-blocks-per-chunk)
        (close-writers! this)

        (set! (.chunk-idx this) (inc chunk-idx))))

    Closeable
    (close [this]
      #_
      ;; we don't want to close a chunk just because this node's shutting down?
      ;; will make the ingester non-deterministic...
      (close-writers! this)

      (doseq [^VectorSchemaRoot root (vals roots)]
        (.close root))))

  (defn- close-writers! [^Ingester ingester]
    (let [^Map file-writers (.file-writers ingester)]
      (doseq [[k ^ArrowFileWriter file-writer] file-writers]
        (let [^VectorSchemaRoot root (get (.roots ingester) k)]
          (when (pos? (.getRowCount root))
            (.writeBatch file-writer)

            (doseq [^FieldVector field-vector (.getFieldVectors root)]
              (.reset field-vector))

            (.setRowCount root 0)))

        (.close file-writer))

      (.clear file-writers)))

  #_
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)
              ingester (Ingester. allocator
                                  (doto (io/file "/tmp/arrow") .mkdirs)
                                  (HashMap.)
                                  (HashMap.)
                                  0)]

    (index-tx ingester foo-tx-bytes)))

(comment
  ;; converting Arrow to JSON
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)]

    (doseq [^File
            file (->> (.listFiles (io/file "/tmp/arrow"))
                      (filter #(.endsWith (.getName ^File %) ".arrow")))]
      (with-open [file-ch (FileChannel/open (.toPath file)
                                            (into-array OpenOption #{StandardOpenOption/READ}))
                  file-reader (ArrowFileReader. file-ch allocator)
                  file-writer (JsonFileWriter. (io/file (format "/tmp/arrow/%s.json" (.getName file)))
                                               (.. (JsonFileWriter/config) (pretty true)))]
        (let [root (.getVectorSchemaRoot file-reader)]
          (.start file-writer (.getSchema root) nil)
          (while (.loadNextBatch file-reader)
            (.write file-writer root)))))))
