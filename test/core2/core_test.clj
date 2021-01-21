(ns core2.core-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.instant :as inst]
            [clojure.string :as str])
  (:import [java.nio.channels FileChannel]
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
;; let's just get a chunk on disk, forget 'live' blocks - many blocks to a chunk, many chunks
;; 1m total, 1k per transaction, 10 transactions per block, 10 blocks per chunk, 10 chunks
;; more than one block per chunk - 'seal' a block, starting a new live block
;; dynamic documents - atm we've hardcoded cols and types
;; metadata - minmax, bloom at chunk/file and block/record-batch

;; once we've sealed a _chunk_ (/ block?), throw away in-memory? mmap or load
;; abstract over this - we don't need to know whether it's mmap'd or in-memory

;; reading a block before it's sealed?
;; theory: if we're only appending, we should be ok?

;; two different cases:
;; live chunk, reading sealed block (maybe) written to disk
;; live chunk, reading unsealed block in memory

(comment
  (with-readings-docs
    (fn [readings]
      (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
        (with-open [battery-levels (Float8Vector. "battery-level" allocator)
                    device-ids (VarCharVector. "device-id" allocator)
                    root (let [^Iterable vecs [battery-levels device-ids]]
                           (VectorSchemaRoot. vecs))]


          (doseq [[chunk-idx chunk-readings] (map-indexed vector (partition-all 100000 readings))]
            (with-open [file-ch (FileChannel/open (.toPath (io/file (format "/tmp/arrow/readings-%d.arrow" chunk-idx)))
                                                  (into-array OpenOption #{StandardOpenOption/CREATE
                                                                           StandardOpenOption/WRITE
                                                                           StandardOpenOption/TRUNCATE_EXISTING}))
                        arrow-file-writer (ArrowFileWriter. root nil file-ch)]
              (.start arrow-file-writer)

              (doseq [block-readings (partition-all 10000 chunk-readings)]
                (.reset battery-levels)
                (.reset device-ids)

                (doseq [[^int idx {:keys [^double battery-level ^String device-id]}] (map-indexed vector block-readings)]
                  (.setSafe battery-levels idx battery-level)
                  (.setSafe device-ids idx (Text. device-id)))

                (.setRowCount root (count block-readings))

                (.writeBatch arrow-file-writer))

              (.end arrow-file-writer))))

        #_
        (with-open [file-ch (FileChannel/open (.toPath (io/file "/tmp/readings.arrow"))
                                              (into-array OpenOption #{StandardOpenOption/READ}))
                    file-reader (ArrowFileReader. file-ch allocator)
                    root (.getVectorSchemaRoot file-reader)]
          (while (.loadNextBatch file-reader)
            (println (.getRowCount root))))
        ))))
