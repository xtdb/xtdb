(ns core2.indexer-test
  (:require [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.core :as c2]
            [core2.json :as c2-json]
            [core2.metadata :as meta]
            [core2.temporal.kd-tree :as kd]
            [core2.test-util :as tu]
            [core2.ts-devices :as ts]
            [core2.tx :as tx]
            [core2.types :as ty]
            [core2.util :as util])
  (:import [core2.buffer_pool BufferPool IBufferPool]
           core2.core.Node
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           core2.temporal.TemporalManager
           [core2.tx TransactionInstant Watermark]
           java.nio.file.Files
           java.time.Duration
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector VarCharVector VectorLoader VectorSchemaRoot]
           org.apache.arrow.vector.complex.StructVector))

(def txs
  [[{:op :put
     :doc {:_id "device-info-demo000000",
           :api-version "23",
           :manufacturer "iobeam",
           :model "pinto",
           :os-name "6.0.1"}}
    {:op :put
     :doc {:_id "reading-demo000000",
           :device-id "device-info-demo000000",
           :cpu-avg-15min 8.654,
           :rssi -50.0,
           :cpu-avg-5min 10.802,
           :battery-status "discharging",
           :ssid "demo-net",
           :time #inst "2016-11-15T12:00:00.000-00:00",
           :battery-level 59.0,
           :bssid "01:02:03:04:05:06",
           :battery-temperature 89.5,
           :cpu-avg-1min 24.81,
           :mem-free 4.10011078E8,
           :mem-used 5.89988922E8}}]
   [{:op :put
     :doc {:_id "device-info-demo000001",
           :api-version "23",
           :manufacturer "iobeam",
           :model "mustang",
           :os-name "6.0.1"}}
    {:op :put
     :doc {:_id "reading-demo000001",
           :device-id "device-info-demo000001",
           :cpu-avg-15min 8.822,
           :rssi -61.0,
           :cpu-avg-5min 8.106,
           :battery-status "discharging",
           :ssid "stealth-net",
           :time #inst "2016-11-15T12:00:00.000-00:00",
           :battery-level 86.0,
           :bssid "A0:B1:C5:D2:E0:F3",
           :battery-temperature 93.7,
           :cpu-avg-1min 4.93,
           :mem-free 7.20742332E8,
           :mem-used 2.79257668E8}}]])

(defn open-watermark ^core2.tx.Watermark [node]
  (.watermark (c2/open-db node)))

(t/deftest can-build-chunk-as-arrow-ipc-file-format
  (let [node-dir (util/->path "target/can-build-chunk-as-arrow-ipc-file-format")
        last-tx-instant (tx/->TransactionInstant 6240 #inst "2020-01-02")
        total-number-of-ops (count (for [tx-ops txs
                                         op tx-ops]
                                     op))]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir
                                       :clock (tu/->mock-clock [#inst "2020-01-01" #inst "2020-01-02"])})]
      (let [system @(:!system node)
            ^BufferAllocator a (:core2/allocator system)
            ^ObjectStore os (:core2/object-store system)
            ^IBufferPool bp (:core2/buffer-pool system)
            ^IMetadataManager mm (:core2/metadata-manager system)
            ^TemporalManager tm (:core2/temporal-manager system)]

        (t/is (every? nil? @(meta/with-latest-metadata mm
                              (juxt meta/latest-tx meta/latest-row-id))))

        (t/is (= last-tx-instant
                 (last (for [tx-ops txs]
                         @(c2/submit-tx node tx-ops)))))

        (t/is (= last-tx-instant
                 (c2/await-tx node last-tx-instant (Duration/ofSeconds 2))))

        (t/testing "watermark"
          (with-open [watermark (open-watermark node)]
            (let [column->root (.column->root watermark)
                  first-column (first column->root)
                  last-column (last column->root)]
              (t/is (zero? (.chunk-idx watermark)))
              (t/is (= 4 (.row-count watermark)))
              (t/is (t/is 20 (count column->root)))
              (t/is (= ["_id" 4]
                       [(key first-column) (.getRowCount ^VectorSchemaRoot (val first-column))]))
              (t/is (= ["time" 2]
                       [(key last-column) (.getRowCount ^VectorSchemaRoot (val last-column))])))))

        (t/testing "temporal"
          (t/is (= {"device-info-demo000000" -4962768465676381896
                    "reading-demo000000" 4437113781045784766
                    "device-info-demo000001" -6688467811848818630
                    "reading-demo000001" -8292973307042192125}
                   (.id->internal-id tm)))
          (with-open [watermark (open-watermark node)]
            (t/is (= 4 (count (kd/kd-tree->seq (.temporal-watermark watermark)))))))

        (tu/finish-chunk node)

        (with-open [watermark (open-watermark node)]
          (t/is (= 4 (.chunk-idx watermark)))
          (t/is (zero? (.row-count watermark)))
          (t/is (empty? (.column->root watermark))))

        (t/is (= [last-tx-instant (dec total-number-of-ops)]
                 @(meta/with-latest-metadata mm
                    (juxt meta/latest-tx meta/latest-row-id))))

        (let [objects-list (.listObjects os "metadata-")]
          (t/is (= 1 (count objects-list)))
          (t/is (= "metadata-00000000.arrow" (first objects-list))))

        (tu/check-json (.toPath (io/as-file (io/resource "can-build-chunk-as-arrow-ipc-file-format"))) os)

        (t/testing "buffer pool"
          (let [buffer-name "metadata-00000000.arrow"
                ^ArrowBuf buffer @(.getBuffer bp buffer-name)
                footer (util/read-arrow-footer buffer)]
            (t/is (= 2 (count (.buffers ^BufferPool bp))))
            (t/is (instance? ArrowBuf buffer))
            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (with-open [^ArrowBuf same-buffer @(.getBuffer bp buffer-name)]
              (t/is (identical? buffer same-buffer))
              (t/is (= 3 (.getRefCount (.getReferenceManager ^ArrowBuf buffer)))))

            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (t/is (= meta/metadata-schema (.getSchema footer)))
            (t/is (= 1 (count (.getRecordBatches footer))))
            (with-open [^VectorSchemaRoot metadata-batch (VectorSchemaRoot/create (.getSchema footer) a)
                        record-batch (util/->arrow-record-batch-view (first (.getRecordBatches footer)) buffer)]
              (.load (VectorLoader. metadata-batch) record-batch)
              (t/is (= 20 (.getRowCount metadata-batch)))
              (t/is (= "_id" (-> (.getVector metadata-batch "column")
                                 (ty/get-object 0))))
              (t/is (= "device-info-demo000000"
                       (-> ^StructVector (.getVector metadata-batch "min")
                           ^VarCharVector (.getChild "varchar")
                           (ty/get-object 0))))
              (t/is (= "reading-demo000001"
                       (-> ^StructVector (.getVector metadata-batch "max")
                           ^VarCharVector (.getChild "varchar")
                           (ty/get-object 0))))
              (t/is (= 4 (-> ^BigIntVector (.getVector metadata-batch "count")
                             (.get 0))))

              (let [from (.getVector metadata-batch "count")
                    tp (.getTransferPair from a)]
                (with-open [to (.getTo tp)]
                  (t/is (zero? (.getValueCount to)))
                  (.splitAndTransfer tp 0 20)
                  (t/is  (= (.memoryAddress (.getDataBuffer from))
                            (.memoryAddress (.getDataBuffer to))))
                  (t/is (= 20 (.getValueCount to))))))

            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (let [size (.getSize (.getReferenceManager ^ArrowBuf buffer))]
              (= size (.getAccountedSize (.getReferenceManager ^ArrowBuf buffer)))
              (.close buffer)
              (t/is (= 1 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

              (t/is (true? (.evictBuffer bp buffer-name)))
              (t/is (false? (.evictBuffer bp buffer-name)))
              (t/is (zero? (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))
              (t/is (= size (.getSize (.getReferenceManager ^ArrowBuf buffer))))
              (t/is (zero? (.getAccountedSize (.getReferenceManager ^ArrowBuf buffer))))
              (t/is (= 1 (count (.buffers ^BufferPool bp)))))))))))

(t/deftest can-handle-dynamic-cols-in-same-block
  (let [node-dir (util/->path "target/can-handle-dynamic-cols-in-same-block")
        mock-clock (tu/->mock-clock [#inst "2020-01-01" #inst "2020-01-02" #inst "2020-01-03"])
        tx-ops [{:op :put, :doc {:_id "foo"}}
                {:op :put, :doc {:_id 24.0}}
                {:op :put, :doc {:_id "bar"}}
                {:op :put, :doc {:_id #inst "2021-01-01"}}
                {:op :put, :doc {:_id 52.0}}
                {:op :put, :doc {:_id #inst "2020-01-01"}}]]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :clock mock-clock})]
      (let [^ObjectStore os (:core2/object-store @(:!system node))]

        @(-> (c2/submit-tx node tx-ops)
             (tu/then-await-tx node))

        (tu/finish-chunk node)

        (tu/check-json (.toPath (io/as-file (io/resource "can-handle-dynamic-cols-in-same-block"))) os)))))

(t/deftest can-stop-node-without-writing-chunks
  (let [node-dir (util/->path "target/can-stop-node-without-writing-chunks")
        mock-clock (tu/->mock-clock [#inst "2020-01-01" #inst "2020-01-02"])
        last-tx-instant (tx/->TransactionInstant 6240 #inst "2020-01-02")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :clock mock-clock})]
      (let [object-dir (.resolve node-dir "objects")]

        (t/is (= last-tx-instant
                 (last (for [tx-ops txs]
                         @(c2/submit-tx node tx-ops)))))

        (t/is (= last-tx-instant
                 (c2/await-tx node last-tx-instant (Duration/ofSeconds 2))))
        (t/is (= last-tx-instant (c2/latest-completed-tx node)))

        (with-open [node (tu/->local-node {:node-dir node-dir})]
          (t/is (= last-tx-instant
                   (c2/await-tx node last-tx-instant (Duration/ofSeconds 2))))
          (t/is (= last-tx-instant (c2/latest-completed-tx node))))

        (t/is (zero? (.count (Files/list object-dir))))))))

(t/deftest can-ingest-ts-devices-mini
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :max-rows-per-chunk 3000, :max-rows-per-block 300})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [^ObjectStore os (:core2/object-store @(:!system node))
            ^IMetadataManager mm (:core2/metadata-manager @(:!system node))
            device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     {:op :put
                      :doc doc})]

        (t/is (= 11000 (count tx-ops)))

        (t/is (nil? (c2/latest-completed-tx node)))

        (let [last-tx-instant @(reduce
                                (fn [_acc tx-ops]
                                  (c2/submit-tx node tx-ops))
                                nil
                                (partition-all 100 tx-ops))]

          (t/is (= last-tx-instant (c2/await-tx node last-tx-instant (Duration/ofSeconds 5))))
          (t/is (= last-tx-instant (c2/latest-completed-tx node)))
          (tu/finish-chunk node)

          (t/is [last-tx-instant (dec (count tx-ops))]
                @(meta/with-latest-metadata mm
                   (juxt meta/latest-tx meta/latest-row-id)))

          (let [objs (.listObjects os)]
            (t/is (= 4 (count (filter #(re-matches #"temporal-\p{XDigit}+.*" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"temporal-snapshot-\p{XDigit}+.*" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"metadata-.*" %) objs))))
            (t/is (= 1 (count (filter #(re-matches #"chunk-.*-api-version.*" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"chunk-.*-battery-level.*" %) objs))))))))

    (c2-json/write-arrow-json-files (.toFile (.resolve node-dir "objects")))

    (t/testing "blocks are row-id aligned"
      (letfn [(row-id-ranges [^String file-name]
                (let [path (.resolve (.resolve node-dir "objects") file-name)]
                  (for [batch (-> (Files/readString path)
                                  json/parse-string
                                  (get "batches"))
                        :let [data (-> (get batch "columns")
                                       first
                                       (get "DATA"))]]
                    [(Long/parseLong (first data))
                     (Long/parseLong (last data))
                     (count data)])))]

        (t/is (= [[0 299 300] [300 599 300] [600 899 300] [900 1199 300]
                  [1200 1499 300] [1500 1799 300] [1800 2099 300]
                  [2100 2399 300] [2400 2699 300] [2700 2999 300]]
                 (row-id-ranges "chunk-00000000-_tx-id.arrow.json")))

        (t/is (= [[0 298 150] [300 598 150] [600 898 150] [900 1198 150]
                  [1200 1498 150] [1500 1798 150] [1800 1998 100]]
                 (row-id-ranges "chunk-00000000-api-version.arrow.json")))

        (t/is (= [[1 299 150] [301 599 150] [601 899 150] [901 1199 150]
                  [1201 1499 150] [1501 1799 150] [1801 2099 200]
                  [2100 2399 300] [2400 2699 300] [2700 2999 300]]
                 (row-id-ranges "chunk-00000000-battery-level.arrow.json")))

        (t/is (= [[3000 3299 300] [3300 3599 300] [3600 3899 300] [3900 4199 300]
                  [4200 4499 300] [4500 4799 300] [4800 5099 300]
                  [5100 5399 300] [5400 5699 300] [5700 5999 300]]
                 (row-id-ranges "chunk-00000bb8-_tx-id.arrow.json")))

        (t/is (= [[3000 3299 300] [3300 3599 300] [3600 3899 300] [3900 4199 300]
                  [4200 4499 300] [4500 4799 300] [4800 5099 300]
                  [5100 5399 300] [5400 5699 300] [5700 5999 300]]
                 (row-id-ranges "chunk-00000bb8-battery-level.arrow.json")))))))

(t/deftest can-ingest-ts-devices-mini-into-multiple-nodes
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini-into-multiple-nodes")
        node-opts {:node-dir node-dir, :max-rows-per-chunk 1000, :max-rows-per-block 100}]
    (util/delete-dir node-dir)

    (with-open [node-1 (tu/->local-node (assoc node-opts :buffers-dir "buffers-1"))
                node-2 (tu/->local-node (assoc node-opts :buffers-dir "buffers-2"))
                node-3 (tu/->local-node (assoc node-opts :buffers-dir "buffers-3"))
                submit-node (tu/->local-submit-node {:node-dir node-dir})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     {:op :put
                      :doc doc})]

        (t/is (= 11000 (count tx-ops)))

        (let [last-tx-instant @(reduce
                                (fn [_ tx-ops]
                                  (c2/submit-tx submit-node tx-ops))
                                nil
                                (partition-all 100 tx-ops))]

          (doseq [^Node node (shuffle (take 6 (cycle [node-1 node-2 node-3])))
                  :let [os ^ObjectStore (:core2/object-store @(:!system node))]]
            (t/is (= last-tx-instant (c2/await-tx node last-tx-instant (Duration/ofSeconds 10))))
            (t/is (= last-tx-instant (c2/latest-completed-tx node)))

            (Thread/sleep 1000) ;; TODO for now

            (let [objs (.listObjects os)]
              (t/is (= 11 (count (filter #(re-matches #"temporal-\p{XDigit}+.*" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"temporal-snapshot-\p{XDigit}+.*" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"metadata-.*" %) objs))))
              (t/is (= 2 (count (filter #(re-matches #"chunk-.*-api-version.*" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"chunk-.*-battery-level.*" %) objs)))))))))))

(t/deftest can-ingest-ts-devices-mini-with-stop-start-and-reach-same-state
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini-with-stop-start-and-reach-same-state")
        node-opts {:node-dir node-dir, :max-rows-per-chunk 1000, :max-rows-per-block 100}]
    (util/delete-dir node-dir)

    (with-open [submit-node (tu/->local-submit-node {:node-dir node-dir})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     {:op :put
                      :doc doc})
            [first-half-tx-ops second-half-tx-ops] (split-at (/ (count tx-ops) 2) tx-ops)]

        (t/is (= 5500 (count first-half-tx-ops)))
        (t/is (= 5500 (count second-half-tx-ops)))

        (let [^TransactionInstant
              first-half-tx-instant @(reduce
                                      (fn [_ tx-ops]
                                        (c2/submit-tx submit-node tx-ops))
                                      nil
                                      (partition-all 100 first-half-tx-ops))]

          (with-open [node (tu/->local-node (assoc node-opts :buffers-dir "buffers-1"))]
            (let [system @(:!system node)
                  ^ObjectStore os (:core2/object-store system)
                  ^IMetadataManager mm (:core2/metadata-manager system)
                  ^TemporalManager tm (:core2/temporal-manager system)]
              (t/is (= first-half-tx-instant (c2/await-tx node first-half-tx-instant (Duration/ofSeconds 5))))
              (t/is (= first-half-tx-instant (c2/latest-completed-tx node)))

              (let [[^TransactionInstant os-tx-instant os-latest-row-id] @(meta/with-latest-metadata mm
                                                                            (juxt meta/latest-tx meta/latest-row-id))]
                (t/is (< (.tx-id os-tx-instant) (.tx-id first-half-tx-instant)))
                (t/is (< os-latest-row-id (count first-half-tx-ops)))

                (let [objs (.listObjects os)]
                  (t/is (= 5 (count (filter #(re-matches #"metadata-.*" %) objs))))
                  (t/is (= 2 (count (filter #(re-matches #"chunk-.*-api-version.*" %) objs))))
                  (t/is (= 5 (count (filter #(re-matches #"chunk-.*-battery-level.*" %) objs)))))

                (t/is (= 2000 (count (.id->internal-id tm)))))

              (let [^TransactionInstant
                    second-half-tx-instant @(reduce
                                             (fn [_ tx-ops]
                                               (c2/submit-tx submit-node tx-ops))
                                             nil
                                             (partition-all 100 second-half-tx-ops))]

                (t/is (<= (.tx-id first-half-tx-instant)
                          (.tx-id (c2/latest-completed-tx node))
                          (.tx-id second-half-tx-instant)))

                (with-open [new-node (tu/->local-node (assoc node-opts :buffers-dir "buffers-2"))]
                  (doseq [^Node node [new-node node]
                          :let [^TemporalManager tm (:core2/temporal-manager @(:!system node))]]

                    (t/is (<= (.tx-id first-half-tx-instant)
                              (.tx-id (c2/await-tx node first-half-tx-instant (Duration/ofSeconds 5)))
                              (.tx-id second-half-tx-instant)))

                    (with-open [watermark (open-watermark node)]
                      (t/is (>= (count (kd/kd-tree->seq (.temporal-watermark watermark))) 3500))
                      (t/is (>= (count (.id->internal-id tm)) 2000))))

                  (doseq [^Node node [new-node node]]
                    (t/is (= second-half-tx-instant (c2/await-tx node second-half-tx-instant (Duration/ofSeconds 5))))
                    (t/is (= second-half-tx-instant (c2/latest-completed-tx node))))

                  (Thread/sleep 1000) ;; for now

                  (doseq [^Node node [new-node node]
                          :let [^ObjectStore os (:core2/object-store @(:!system node))
                                ^TemporalManager tm (:core2/temporal-manager @(:!system node))]]

                    (let [objs (.listObjects os)]
                      (t/is (= 11 (count (filter #(re-matches #"temporal-\p{XDigit}+.*" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"temporal-snapshot-\p{XDigit}+.*" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"metadata-.*" %) objs))))
                      (t/is (= 2 (count (filter #(re-matches #"chunk-.*-api-version.*" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"chunk-.*-battery-level.*" %) objs)))))

                    (t/is (= 2000 (count (.id->internal-id tm))))))))))))))
