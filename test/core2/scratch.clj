(ns core2.scratch
  (:require [clojure.data.csv :as csv]
            [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [core2.core :as c2]
            [core2.json :as c2-json]
            [core2.metadata :as c2-md])
  (:import java.io.File
           [java.nio.channels FileChannel]
           [java.nio.file Files OpenOption StandardOpenOption]
           java.util.Date
           [org.apache.arrow.vector BigIntVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.UnionVector
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.util.Text))

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
;; DONE object store protocol, store chunks and metadata. Local directory implementation.
;; TODO log protocol. Local directory implementation.
;; TODO figure out last tx-id/row-id from latest chunk and resume ingest on start.
;; TODO (bonus) refactor ingest to use holders / simplify union code.
;; TODO (bonus) unnest ops similar to metadata.
;; TODO (bonus) blocks vs chunks in object store?

;;; query
;; TODO 7. reading any blocks - select battery_level from db (simple code-level query)
;; TODO 7b. fetch metadata and find further chunks based on metadata.
;; TODO 8. reading live blocks
;; TODO 8a. reading chunks already written to disk
;; TODO 8b. reading blocks already written to disk
;; TODO 8c. reading current block not yet written to disk
;; TODO 8d. VSR committed read slice

;;; future
;; TODO dealing with schema that changes throughout an ingest (promotable unions, including nulls)
;; TODO metadata
;; TODO   block-level metadata - where do we store it?
;; TODO   bloom filters in metadata.
;; TODO dictionaries
;; TODO consider eviction
;; TODO writer?
;; TODO handle deletes
;; TODO JMH + JSON tests
;; TODO vectorised operations in the ingester
;; TODO tx-ids?

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


(comment
  (with-open [a (RootAllocator. Long/MAX_VALUE)
              r (VectorSchemaRoot/create c2-md/metadata-schema a)]
    (.setSafe ^VarCharVector (.getVector r "file") 0 (Text. "00000"))
    (.setSafe ^VarCharVector (.getVector r "column") 0 (Text. "name"))
    (.setSafe ^VarCharVector (.getVector r "field") 0 (Text. "name"))
    (let [min-vec ^UnionVector (.getVector r "min")]
      (.setType  min-vec 0 Types$MinorType/VARCHAR)
      (.setSafe (.getVarCharVector min-vec) 0 (Text. "Aaron")))
    (let [max-vec ^UnionVector (.getVector r "max")]
      (.setType max-vec 0 Types$MinorType/VARCHAR)
      (.setSafe (.getVarCharVector max-vec) 0 (Text. "Zach")))
    (.setSafe ^BigIntVector (.getVector r "count") 0 2)
    (.setRowCount r 1)

    (.setSafe ^VarCharVector (.getVector r "file") 1 (Text. "00000"))
    (.setSafe ^VarCharVector (.getVector r "column") 1 (Text. "name"))
    (.setSafe ^VarCharVector (.getVector r "field") 1 (Text. "_row-id"))
    (let [min-vec ^UnionVector (.getVector r "min")]
      (.setType  min-vec 1 Types$MinorType/BIGINT)
      (.setSafe (.getBigIntVector min-vec) 1 1))
    (let [max-vec ^UnionVector (.getVector r "max")]
      (.setType max-vec 1 Types$MinorType/BIGINT)
      (.setSafe (.getBigIntVector max-vec) 1 2))
    (.setSafe ^BigIntVector (.getVector r "count") 1 2)
    (.setRowCount r 2)

    (.setSafe ^VarCharVector (.getVector r "file") 2 (Text. "00000"))
    (.setSafe ^VarCharVector (.getVector r "column") 2 (Text. "age"))
    (.setSafe ^VarCharVector (.getVector r "field") 2 (Text. "age"))
    (let [min-vec ^UnionVector (.getVector r "min")]
      (.setType min-vec 2 Types$MinorType/BIGINT)
      (.setSafe (.getBigIntVector min-vec) 2 10))
    (let [max-vec ^UnionVector (.getVector r "max")]
      (.setType max-vec 2 Types$MinorType/BIGINT)
      (.setSafe (.getBigIntVector max-vec) 2 20))
    (.setSafe ^BigIntVector (.getVector r "count") 2 2)
    (.setRowCount r 3)

    (.setSafe ^VarCharVector (.getVector r "file") 3 (Text. "00000"))
    (.setSafe ^VarCharVector (.getVector r "column") 3 (Text. "age"))
    (.setSafe ^VarCharVector (.getVector r "field") 3 (Text. "_row-id"))
    (let [min-vec ^UnionVector (.getVector r "min")]
      (.setType  min-vec 3 Types$MinorType/BIGINT)
      (.setSafe (.getBigIntVector min-vec) 3 1))
    (let [max-vec ^UnionVector (.getVector r "max")]
      (.setType max-vec 3 Types$MinorType/BIGINT)
      (.setSafe (.getBigIntVector max-vec) 3 2))
    (.setSafe ^BigIntVector (.getVector r "count") 3 2)
    (.setRowCount r 4)

    (.contentToTSVString r)))
