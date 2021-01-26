(ns core2.core-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.instant :as inst]
            [clojure.string :as str])
  (:import [clojure.lang Keyword]
           [java.io Closeable File]
           [java.nio.channels FileChannel]
           [java.nio.file OpenOption StandardOpenOption]
           [java.util Date HashMap Map]
           [java.util.function Function]
           [org.apache.arrow.memory ArrowBuf BufferAllocator RootAllocator]
           [org.apache.arrow.vector FieldVector DateMilliVector Float8Vector VarCharVector VectorLoader VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector MapVector StructVector]
           [org.apache.arrow.vector.complex.impl VarCharWriterImpl DenseUnionWriter]
           [org.apache.arrow.vector.holders DateMilliHolder Float8Holder VarCharHolder UInt8Holder]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowFileWriter JsonFileReader JsonFileWriter JsonFileWriter$JSONWriteConfig ReadChannel]
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
            :device-id device-id
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

(do
  (def tx-arrow-schema
    (Schema. [(->field "tx-ops" ArrowType$List/INSTANCE false
                       (->field "tx-op" (ArrowType$Union. UnionMode/Dense (int-array [0 1])) true
                                (->field "put" (.getType Types$MinorType/STRUCT) false
                                         (->field "document" ArrowType$List/INSTANCE false
                                                  (->field "entries" (.getType Types$MinorType/STRUCT) false
                                                           (->field "key" (.getType Types$MinorType/VARCHAR) false)
                                                           (->field "value" (.getType Types$MinorType/VARCHAR) false)))
                                         (->field "from-valid-time" (.getType Types$MinorType/DATEMILLI) true)
                                         (->field "to-valid-time" (.getType Types$MinorType/DATEMILLI) true))
                                (->field "delete" (.getType Types$MinorType/NULL) true)))]))

  (defn tx->tx-arrow [tx-ops ^RootAllocator allocator]
    (with-open [root (VectorSchemaRoot/create tx-arrow-schema allocator)]
      (let [^ListVector tx-ops-vec (.getVector root "tx-ops")
            ^DenseUnionVector tx-op-vec (.getDataVector tx-ops-vec)

            union-type-ids (into {} (map vector
                                         (->> (.getChildren (.getField tx-op-vec))
                                              (map #(.getName ^Field %)))
                                         (range)))
            put-type-id (get union-type-ids "put")

            ^StructVector put-vec (.getStruct tx-op-vec put-type-id)
            ^ListVector put-doc-vec (.getChild put-vec "document" ListVector)
            ^StructVector put-doc-entry-vec (.getDataVector put-doc-vec)
            ^VarCharVector put-doc-entry-k-vec (.getChild put-doc-entry-vec "key" VarCharVector)
            ^VarCharVector put-doc-entry-v-vec (.getChild put-doc-entry-vec "value" VarCharVector)
            ^DateMilliVector start-vt-vec (.getChild put-vec "from-valid-time" DateMilliVector)
            ^DateMilliVector end-vt-vec (.getChild put-vec "to-valid-time" DateMilliVector)]

        (.setRowCount root 1)

        (let [tx-ops-offset (.startNewValue tx-ops-vec 0)]
          (.setValueCount tx-op-vec (count tx-ops))
          (doseq [[op-type tx-ops] (->> (map-indexed vector tx-ops)
                                        (group-by (comp (fn [_]
                                                          (rand-nth ["put" "delete"]))
                                                        second)))
                  [union-offset-idx [tx-op-idx tx-op]] (map-indexed vector tx-ops)
                  :let [^int op-idx (+ tx-ops-offset tx-op-idx)]]

            (.setTypeId tx-op-vec op-idx (get union-type-ids op-type))
            (.setInt (.getOffsetBuffer tx-op-vec) (* DenseUnionVector/OFFSET_WIDTH tx-op-idx) union-offset-idx)

            (when (= op-type "put")
              (.setIndexDefined put-vec op-idx)

              (.setSafe start-vt-vec op-idx (.getTime (java.util.Date.)))
              (.setSafe end-vt-vec op-idx (.getTime (java.util.Date.)))

              (let [doc-offset (.startNewValue put-doc-vec op-idx)]
                (doseq [[kv-idx [k v]] (map-indexed vector tx-op)
                        :let [^int kv-idx (+ doc-offset kv-idx)]]
                  (.setNotNull put-doc-vec op-idx)
                  (.setIndexDefined put-doc-entry-vec kv-idx)
                  (.setSafe put-doc-entry-k-vec kv-idx (Text. (name k)))
                  (.setSafe put-doc-entry-v-vec kv-idx (Text. (pr-str v))))
                (.endValue put-doc-vec op-idx (count tx-op)))))
          #_
          (.endValue tx-ops-vec 0 (count tx-ops)))
        #_
        (.getObject tx-ops-vec put-type-id))

      ;; TODO DenseUnionVector isn't setting the offset buffer without writers. sad.

      #_
      (with-open [augmented-root (VectorSchemaRoot. (concat (.getFieldVectors root)
                                                            [(.createVector (->field "tx-time" (.getType Types$MinorType/DATEMILLI) false))
                                                             (.createVector (->field "tx-id" (.getType Types$MinorType/UINT8) false))
                                                             ]))]
        augmented-root)))

  (defn tx-arrow->ingest-log-arrow []
    )

  #_
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
    (with-readings-docs
      (fn [readings]
        (tx->tx-arrow (take 10 readings) allocator)))))

;; idea: tx-log only needs to store row-ids, can then be re-created from the content indices if required

(do
  (deftype Ingester [^BufferAllocator allocator
                     ^File arrow-dir
                     ^Map roots ; (hash keys) -> VectorSchemaRoot
                     ^Map file-writers ; (hash keys) -> ArrowFileWriter
                     ^:unsynchronized-mutable ^long chunk-idx]

    TransactionIngester
    (index-tx [this docs] ; TODO eventually docs :: List<ArrowSomething> rather than List<IPersistentMap>
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

            (.setRowCount root 0))))

      ;; TODO better metric here?
      (when (>= (->> (vals file-writers)
                     (transduce (map (fn [^ArrowFileWriter file-writer]
                                       (count (.getRecordBlocks file-writer))))
                                +))
                max-blocks-per-chunk)
        (close-writers! this)

        (set! (.chunk-idx this) (inc chunk-idx))))

    Closeable
    (close [this]
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

    (let [info-docs @!info-docs]
      (with-readings-docs
        (fn [readings]
          (doseq [tx (->> (concat (interleave info-docs
                                              (take (count info-docs) readings))
                                  (drop (count info-docs) readings))
                          (partition-all 1000))]
            (index-tx ingester tx)))))

    #_
    (let [key-set #{:cpu-avg-15min :device-id :rssi :cpu-avg-5min :battery-status :ssid :time :battery-level :bssid :battery-temperature :cpu-avg-1min :mem-free :mem-used}
          live-root (-> ^Map (.roots ingester)
                        ^VectorSchemaRoot (.get key-set))]
      [(.refCnt (.getDataBuffer (.getVector live-root ":cpu-avg-15min")))
       (.memoryAddress (.getDataBuffer (.getVector live-root ":cpu-avg-15min")))
       (with-open [live-root-slice (.slice live-root 0 (/ (.getRowCount live-root) 2))]
         [(.refCnt (.getDataBuffer (.getVector live-root-slice ":cpu-avg-15min")))
          (.memoryAddress (.getDataBuffer (.getVector live-root-slice ":cpu-avg-15min")))])]

      #_(with-open [file-ch (FileChannel/open (.toPath (io/file "/tmp/arrow/chunk-00000001-da8dfa70.arrow"))
                                            (into-array OpenOption #{StandardOpenOption/READ}))
                  read-ch (ReadChannel. file-ch)]

        (.position file-ch 8)

        (let [schema (MessageSerializer/deserializeSchema read-ch)]
          (with-open [read-root (VectorSchemaRoot/create schema allocator)]
            (let [loader (VectorLoader. read-root)

                  ^ArrowBlock
                  block (-> ^Map (.file-writers ingester)
                            ^ArrowFileWriter
                            (.get key-set)
                            (.getRecordBlocks)
                            first)]

              (.position file-ch (.getOffset block))

              (with-open [record-batch (MessageSerializer/deserializeRecordBatch read-ch block allocator)]
                (.load loader record-batch)
                (.get ^Float8Vector (.getVector read-root ":battery-level") 0)))))))
    ))

(comment
  ;; converting Arrow to JSON
  (let [file-name "chunk-00000000-ba085ddb"]
    (with-open [allocator (RootAllocator. Long/MAX_VALUE)
                file-ch (FileChannel/open (.toPath (io/file (format "/tmp/arrow/%s.arrow" file-name)))
                                          (into-array OpenOption #{StandardOpenOption/READ}))
                file-reader (ArrowFileReader. file-ch allocator)
                file-writer (JsonFileWriter. (io/file (format "/tmp/arrow/%s.json" file-name))
                                             (.. (JsonFileWriter/config) (pretty true)))]
      (let [root (.getVectorSchemaRoot file-reader)]
        (.start file-writer (.getSchema root) nil)
        (while (.loadNextBatch file-reader)
          (.write file-writer root))))))
