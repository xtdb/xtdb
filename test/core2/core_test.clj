(ns core2.core-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.instant :as inst]
            [clojure.string :as str])
  (:import [java.io Closeable File]
           [java.nio.channels FileChannel]
           [java.nio.file Path OpenOption StandardOpenOption]
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector FieldVector Float8Vector VarCharVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowFileWriter JsonFileReader JsonFileWriter]
           [org.apache.arrow.vector.util Text]))

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
           (-> {:time (inst/read-instant-date
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
                :ssid ssid}
               (select-keys [:device-id #_:time #_:bssid :battery-level])))))))

(comment
  (defonce foo-rows
    (with-readings-docs
      (fn [rows]
        (doall (take 10 rows))))))

;; Writer API?

;; TODO
;; 1. let's just get a chunk on disk, forget 'live' blocks - many blocks to a chunk, many chunks
;;    1m total, 1k per transaction, 10 transactions per block, 10 blocks per chunk, 10 chunks
;; 1b. writer?
;; 2. more than one block per chunk - 'seal' a block, starting a new live block
;; 3. dynamic documents - atm we've hardcoded cols and types
;; 4. metadata - minmax, bloom at chunk/file and block/record-batch

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

(do
  (deftype Ingester [^File arrow-dir
                     ^VectorSchemaRoot live-block
                     ^:unsynchronized-mutable ^long chunk-idx
                     ^:unsynchronized-mutable ^ArrowFileWriter live-file-writer]

    TransactionIngester
    (index-tx [this docs]
      (when-not live-file-writer
        (let [file-ch (FileChannel/open (.toPath (io/file arrow-dir (format "chunk-%08x.arrow" chunk-idx)))
                                        (into-array OpenOption #{StandardOpenOption/CREATE
                                                                 StandardOpenOption/WRITE
                                                                 StandardOpenOption/TRUNCATE_EXISTING}))]
          (set! (.live-file-writer this)
                (doto (ArrowFileWriter. live-block nil file-ch)
                  (.start)))))

      (let [row-count (.getRowCount live-block)
            ^Float8Vector battery-levels (.getVector live-block "battery-level")
            ^VarCharVector device-ids (.getVector live-block "device-id")]

        (doseq [[^int idx {:keys [^double battery-level ^String device-id]}] (map vector (range row-count (+ row-count (count docs))) docs)]
          ;; TODO make dynamic (#3)
          (.setSafe battery-levels idx battery-level)
          (.setSafe device-ids idx (Text. device-id)))

        (let [row-count (+ row-count (count docs))]
          (.setRowCount live-block row-count)

          (when (>= row-count max-block-size)
            (.writeBatch live-file-writer)

            (doseq [^FieldVector field-vector (.getFieldVectors live-block)]
              (.reset field-vector))

            (.setRowCount live-block 0)

            (when (>= (count (.getRecordBlocks live-file-writer)) max-blocks-per-chunk)
              (.close live-file-writer)
              (set! (.live-file-writer this) nil)
              (set! (.chunk-idx this) (inc chunk-idx)))))))

    Closeable
    (close [_]
      ;; we don't want to close a chunk just because this node's shutting down?
      ;; will make the ingester non-deterministic...
      (when live-file-writer
        (.close live-file-writer))))

  #_
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)
              battery-levels (Float8Vector. "battery-level" allocator)
              device-ids (VarCharVector. "device-id" allocator)
              root (let [^Iterable vecs [battery-levels device-ids]]
                     (VectorSchemaRoot. vecs))

              ingester (Ingester. (io/file "/tmp/arrow")
                                  root
                                  0
                                  nil)]
    (with-readings-docs
      (fn [readings]
        (doseq [tx (partition-all 1000 readings)]
          (index-tx ingester tx))))))

(comment
  (with-open []
    (with-open [allocator (RootAllocator. Long/MAX_VALUE)
                file-ch (FileChannel/open (.toPath (io/file "/tmp/readings.arrow"))
                                          (into-array OpenOption #{StandardOpenOption/READ}))
                file-reader (ArrowFileReader. file-ch allocator)
                root (.getVectorSchemaRoot file-reader)]
      (while (.loadNextBatch file-reader)
        (println (.getRowCount root))))))
