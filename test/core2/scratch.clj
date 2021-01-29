(ns core2.scratch
  (:require [clojure.data.csv :as csv]
            [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [core2.core :as c2]
            [core2.json :as c2-json])
  (:import java.io.File
           [java.nio.channels FileChannel]
           [java.nio.file Files OpenOption StandardOpenOption]
           java.util.Date
           org.apache.arrow.memory.RootAllocator))

(def device-info-csv-resource
  (io/resource "devices_small_device_info.csv"))

(def !info-docs
  (delay
    (when device-info-csv-resource
      (with-open [rdr (io/reader device-info-csv-resource)]
        (vec (for [[device-id api-version manufacturer model os-name] (csv/read-csv rdr)]
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
           {:_id (str "reading-" device-id)
            :time (inst/read-instant-date
                   (-> time
                       (str/replace " " "T")
                       (str/replace #"-(\d\d)$" ".000-$1:00")))
            :device-id (str "device-info-" device-id)
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

;;; ingest
;; TODO 4. writing metadata - minmax, bloom at chunk/file and block/record-batch
;; TODO 4b. columnar metadata?
;; TODO 4c. batched metadata?
;; TODO 11. (solo) figure out last tx-id/row-id from latest chunk and resume ingest on start.
;; TODO 13. (solo) log protocol. File implementation.
;; TODO 12. object store protocol, store chunks and metadata. File implementation.

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
;; TODO 4d. block-level metadata - where do we store it?
;; TODO 5. dictionaries
;; TODO 6. consider eviction
;; TODO 1b. writer?
;; TODO 15. handle deletes
;; TODO 16. JMH + JSON tests
;; TODO 17. vectorised operations in the ingester

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

(defn write-tx-bytes [^File dir]
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
    (with-readings-docs
      (fn [readings]
        (let [info-docs @!info-docs]
          (doseq [[idx tx-batch] (->> (concat (interleave info-docs
                                                          (take (count info-docs) readings))
                                              (drop (count info-docs) readings))
                                      (partition-all 1000)
                                      (map-indexed vector))]
            (io/copy (c2/submit-tx (for [doc tx-batch]
                                     {:op :put, :doc doc})
                                   allocator)
                     (io/file (doto dir .mkdirs) (format "%08x.stream.arrow" idx)))))))))

#_
(write-tx-bytes (io/file "data/tx-log"))

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

;; aim: one metadata file per chunk with:
;; - count per column
;; - min-max per column

(comment
  (let [arrow-dir (doto (io/file "/tmp/arrow")
                    .mkdirs)]
    (with-open [allocator (RootAllocator. Long/MAX_VALUE)
                ingester (c2/->ingester allocator arrow-dir)]
      (doseq [[tx-id ^File file] (->> (sort (.listFiles (io/file "data/tx-log")))
                                      (map-indexed vector))]
        (c2/index-tx ingester
                     {:tx-id tx-id, :tx-time (Date.)}
                     (Files/readAllBytes (.toPath file)))))

    (c2-json/write-arrow-json-files arrow-dir)))
