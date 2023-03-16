(ns core2.indexer-test
  (:require [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [core2.datalog :as c2.d]
            [core2.api :as c2.api]
            [core2.sql :as c2.sql]
            [core2.buffer-pool :as bp]
            [core2.expression.metadata :as expr.meta]
            [core2.indexer :as idx]
            core2.indexer.internal-id-manager
            [core2.metadata :as meta]
            [core2.node :as node]
            [core2.object-store :as os]
            [core2.test-json :as tj]
            [core2.test-util :as tu]
            [core2.ts-devices :as ts]
            [core2.types :as ty]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            core2.watermark)
  (:import core2.api.TransactionInstant
           [core2.buffer_pool BufferPool IBufferPool]
           (core2.indexer  IIndexer)
           (core2.indexer.internal_id_manager InternalIdManager)
           (core2.metadata IMetadataManager)
           core2.node.Node
           core2.object_store.ObjectStore
           (core2.watermark IWatermark IWatermarkSource)
           java.nio.file.Files
           java.time.Duration
           (java.util.function Consumer)
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector VectorLoader VectorSchemaRoot]
           [org.apache.arrow.vector.complex StructVector]))

(t/use-fixtures :once tu/with-allocator)

(def txs
  [[[:put {:id "device-info-demo000000",
           :api-version "23",
           :manufacturer "iobeam",
           :model "pinto",
           :os-name "6.0.1"}]
    [:put {:id "reading-demo000000",
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
           :mem-used 5.89988922E8}]]
   [[:put {:id "device-info-demo000001",
           :api-version "23",
           :manufacturer "iobeam",
           :model "mustang",
           :os-name "6.0.1"}]
    [:put {:id "reading-demo000001",
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
           :mem-used 2.79257668E8}]]])

(t/deftest can-build-chunk-as-arrow-ipc-file-format
  (let [node-dir (util/->path "target/can-build-chunk-as-arrow-ipc-file-format")
        last-tx-key (c2.api/map->TransactionInstant {:tx-id 8349, :sys-time (util/->instant #inst "2020-01-02")})
        total-number-of-ops (count (for [tx-ops txs
                                         op tx-ops]
                                     op))]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (let [^BufferAllocator a (tu/component node :core2/allocator)
            ^ObjectStore os (tu/component node ::os/file-system-object-store)
            ^IBufferPool bp (tu/component node ::bp/buffer-pool)
            mm (tu/component node ::meta/metadata-manager)
            ^IWatermarkSource wm-src (tu/component node :core2/indexer)]

        (t/is (nil? (meta/latest-chunk-metadata mm)))

        (t/is (= last-tx-key
                 (last (for [tx-ops txs]
                         (c2.d/submit-tx node tx-ops)))))

        (t/is (= last-tx-key
                 (tu/then-await-tx* last-tx-key node (Duration/ofSeconds 2))))

        (t/testing "watermark"
          (with-open [^IWatermark watermark (.openWatermark wm-src last-tx-key)]
            (let [live-blocks (-> (.liveChunk watermark)
                                  (.liveTable "xt_docs")
                                  (.liveBlocks ["id"] nil))
                  !res (volatile! [])]
              (.forEachRemaining live-blocks
                                 (reify Consumer
                                   (accept [_ content-cols]
                                     (vswap! !res conj (set (keys content-cols))))))

              (t/is (= [#{"id"}] @!res)))))

        (tu/finish-chunk! node)

        (t/is (= {:latest-completed-tx last-tx-key
                  :latest-row-id (dec total-number-of-ops)}
                 (-> (meta/latest-chunk-metadata mm)
                     (select-keys [:latest-completed-tx :latest-row-id]))))

        (let [objects-list (->> (.listObjects os) (filter #(str/ends-with? % "/metadata.arrow")))]
          (t/is (= 1 (count objects-list)))
          (t/is (= "chunk-00/xt_docs/metadata.arrow" (first objects-list))))

        (tj/check-json (.toPath (io/as-file (io/resource "can-build-chunk-as-arrow-ipc-file-format")))
                       (.resolve node-dir "objects"))

        (t/testing "buffer pool"
          (let [buffer-name "chunk-00/xt_docs/metadata.arrow"
                ^ArrowBuf buffer @(.getBuffer bp buffer-name)
                footer (util/read-arrow-footer buffer)]
            (t/is (= 2 (count (.buffers ^BufferPool bp))))
            (t/is (instance? ArrowBuf buffer))
            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (with-open [^ArrowBuf same-buffer @(.getBuffer bp buffer-name)]
              (t/is (identical? buffer same-buffer))
              (t/is (= 3 (.getRefCount (.getReferenceManager ^ArrowBuf buffer)))))

            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (t/is (= 1 (count (.getRecordBatches footer))))
            (with-open [^VectorSchemaRoot metadata-batch (VectorSchemaRoot/create (.getSchema footer) a)
                        record-batch (util/->arrow-record-batch-view (first (.getRecordBatches footer)) buffer)]
              (.load (VectorLoader. metadata-batch) record-batch)
              (t/is (= 36 (.getRowCount metadata-batch)))
              (let [id-col-idx (-> (meta/->table-metadata metadata-batch (meta/->table-metadata-idxs metadata-batch))
                                   (.rowIndex "id" -1))]
                (t/is (= "id" (-> (.getVector metadata-batch "column")
                                  (ty/get-object id-col-idx))))
                (let [^StructVector utf8-type-vec (-> ^StructVector (.getVector metadata-batch "types")
                                                      (.getChild "utf8"))]
                  (t/is (= "device-info-demo000000"
                           (-> (.getChild utf8-type-vec "min")
                               (ty/get-object id-col-idx))))
                  (t/is (= "reading-demo000001"
                           (-> (.getChild utf8-type-vec "max")
                               (ty/get-object id-col-idx)))))

                (t/is (= 4 (-> ^BigIntVector (.getVector metadata-batch "count")
                               (.get id-col-idx)))))

              (let [from (.getVector metadata-batch "count")
                    tp (.getTransferPair from a)]
                (with-open [to (.getTo tp)]
                  (t/is (zero? (.getValueCount to)))
                  (.splitAndTransfer tp 0 18)
                  (t/is  (= (.memoryAddress (.getDataBuffer from))
                            (.memoryAddress (.getDataBuffer to))))
                  (t/is (= 18 (.getValueCount to))))))

            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (let [size (.getSize (.getReferenceManager ^ArrowBuf buffer))]
              (t/is (= size (.getAccountedSize (.getReferenceManager ^ArrowBuf buffer))))
              (.close buffer)
              (t/is (= 1 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

              (t/is (true? (.evictBuffer bp buffer-name)))
              (t/is (false? (.evictBuffer bp buffer-name)))
              (t/is (zero? (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))
              (t/is (= size (.getSize (.getReferenceManager ^ArrowBuf buffer))))
              (t/is (zero? (.getAccountedSize (.getReferenceManager ^ArrowBuf buffer))))
              (t/is (= 1 (count (.buffers ^BufferPool bp)))))))))))

(t/deftest temporal-watermark-is-immutable-567
  (with-open [node (node/start-node {})]
    (let [{tt :sys-time} (c2.d/submit-tx node [[:put {:id :foo, :version 0}]]
                                         {:app-time-as-of-now? true})]
      (t/is (= [{:id :foo, :version 0,
                 :application_time_start (util/->zdt tt)
                 :application_time_end (util/->zdt util/end-of-time)
                 :system_time_start (util/->zdt tt)
                 :system_time_end (util/->zdt util/end-of-time)}]
               (tu/query-ra '[:scan {:table xt_docs}
                              [id version
                               application_time_start, application_time_end
                               system_time_start, system_time_end]]
                            {:node node})))

      (let [{tt2 :sys-time} (c2.d/submit-tx node [[:put {:id :foo, :version 1}]]
                                            {:app-time-as-of-now? true})]
        (t/is (= [{:id :foo, :version 0,
                   :application_time_start (util/->zdt tt)
                   :application_time_end (util/->zdt util/end-of-time)
                   :system_time_start (util/->zdt tt)
                   :system_time_end (util/->zdt tt2)}
                  {:id :foo, :version 0,
                   :application_time_start (util/->zdt tt)
                   :application_time_end (util/->zdt tt2)
                   :system_time_start (util/->zdt tt2)
                   :system_time_end (util/->zdt util/end-of-time)}
                  {:id :foo, :version 1,
                   :application_time_start (util/->zdt tt2)
                   :application_time_end (util/->zdt util/end-of-time)
                   :system_time_start (util/->zdt tt2)
                   :system_time_end (util/->zdt util/end-of-time)}]
                 (tu/query-ra '[:scan {:table xt_docs, :for-sys-time :all-time}
                                [id version
                                 application_time_start, application_time_end
                                 system_time_start, system_time_end]]
                              {:node node})))

        #_ ; FIXME #567 this sees the updated system_time_end of the first entry
        (t/is (= [{:id :foo, :version 0,
                   :application_time_start (util/->zdt tt)
                   :application_time_end (util/->zdt util/end-of-time)
                   :system_time_start (util/->zdt tt)
                   :system_time_end (util/->zdt util/end-of-time)}]
                 (tu/query-ra '[:scan {:table xt_docs}
                                [id version
                                 application_time_start, application_time_end
                                 system_time_start, system_time_end]]
                              {:node node, :basis {:tx tx}}))
              "re-using the original snapshot should see the same result")))))

(t/deftest can-handle-dynamic-cols-in-same-block
  (let [node-dir (util/->path "target/can-handle-dynamic-cols-in-same-block")
        tx-ops [[:put {:id "foo"
                       :list [12.0 "foo"]}]
                [:put {:id 24.0}]
                [:put {:id "bar"
                       :list [#inst "2020-01-01" false]}]
                [:put {:id #inst "2021-01-01"
                       :struct {:a 1, :b "b"}}]
                [:put {:id 52.0}]
                [:put {:id #inst "2020-01-01"
                       :struct {:a true, :c "c"}}]]]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (-> (c2.d/submit-tx node tx-ops)
          (tu/then-await-tx* node (Duration/ofMillis 2000)))

      (tu/finish-chunk! node)

      (tj/check-json (.toPath (io/as-file (io/resource "can-handle-dynamic-cols-in-same-block")))
                     (.resolve node-dir "objects")))))

(t/deftest test-multi-block-metadata
  (let [node-dir (util/->path "target/multi-block-metadata")
        tx0 [[:put {:id "foo"
                    :list [12.0 "foo"]}]
             [:put {:id #inst "2021-01-01"
                    :struct {:a 1, :b "b"}}]
             [:put {:id "bar"
                    :list [#inst "2020-01-01" false]}]
             [:put {:id 24.0}]]
        tx1 [[:put {:id 52.0}]
             [:put {:id #inst "2020-01-01"
                    :struct {:a true, :b {:c "c", :d "d"}}}]]]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 3})]
      (c2.d/submit-tx node tx0)

      (-> (c2.d/submit-tx node tx1)
          (tu/then-await-tx* node (Duration/ofMillis 200)))

      (tu/finish-chunk! node)

      (tj/check-json (.toPath (io/as-file (io/resource "multi-block-metadata")))
                     (.resolve node-dir "objects"))

      (let [^IMetadataManager mm (tu/component node ::meta/metadata-manager)]
        (t/is (= [:union #{:utf8 [:timestamp-tz :micro "UTC"] :f64}]
                 (.columnType mm "xt_docs" "id")))

        (t/is (= [:list [:union #{:utf8 [:timestamp-tz :micro "UTC"] :f64 :bool}]]
                 (.columnType mm "xt_docs" "list")))

        (t/is (= [:struct '{a [:union #{:bool :i64}]
                            b [:union #{:utf8
                                        [:struct {c :utf8, d :utf8}]}]}]
                 (.columnType mm "xt_docs" "struct")))))))

(t/deftest round-trips-nils
  (with-open [node (node/start-node {})]
    (c2.d/submit-tx node [[:put {:id "nil-bar"
                                 :foo "foo"
                                 :bar nil}]
                          [:put {:id "no-bar"
                                 :foo "foo"}]])
    (t/is (= [{:id "nil-bar", :foo "foo", :bar nil}]
             (c2.d/q node '{:find [id foo bar]
                            :where [[e :id id]
                                    [e :foo foo]
                                    [e :bar bar]]})))))

(t/deftest round-trips-extensions-via-ipc
  (let [node-dir (util/->path "target/round-trips-extensions-via-ipc")
        uuid #uuid "eb965985-de20-4b45-9721-b6a4bbd694bf"]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (-> (c2.d/submit-tx node [[:put {:id :foo, :uuid uuid}]])
          (tu/then-await-tx* node (Duration/ofMillis 2000)))

      (tu/finish-chunk! node)

      (t/is (= #{{:id :foo, :uuid uuid}}
               (set (c2.d/q node '{:find [id uuid]
                                   :where [[id :uuid uuid]]})))))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (t/is (= #{{:id :foo, :uuid uuid}}
               (set (c2.d/q node '{:find [id uuid]
                                   :where [[id :uuid uuid]]})))))))

(t/deftest writes-log-file
  (let [node-dir (util/->path "target/writes-log-file")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (c2.d/submit-tx node [[:put {:id "foo"}]
                            [:put {:id "bar"}]])

      ;; aborted tx shows up in log
      (c2.d/submit-tx node [[:sql "INSERT INTO foo (id, application_time_start, application_time_end) VALUES (1, DATE '2020-01-01', DATE '2019-01-01')"]])

      (-> (c2.d/submit-tx node [[:delete "xt_docs" "foo" {:app-time-start #inst "2020-04-01"}]
                                [:put {:id "bar", :month "april"},
                                 {:app-time-start #inst "2020-04-01"
                                  :app-time-end #inst "2020-05-01"}]])
          (tu/then-await-tx* node))

      (tu/finish-chunk! node)

      (tj/check-json (.toPath (io/as-file (io/resource "writes-log-file")))
                     (.resolve node-dir "objects")))))

(t/deftest can-stop-node-without-writing-chunks
  (let [node-dir (util/->path "target/can-stop-node-without-writing-chunks")
        last-tx-key (c2.api/map->TransactionInstant {:tx-id 8349, :sys-time (util/->instant #inst "2020-01-02")})]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (let [object-dir (.resolve node-dir "objects")]

        (t/is (= last-tx-key
                 (last (for [tx-ops txs]
                         (c2.d/submit-tx node tx-ops)))))

        (t/is (= last-tx-key
                 (tu/then-await-tx* last-tx-key node (Duration/ofSeconds 2))))
        (t/is (= last-tx-key (tu/latest-completed-tx node)))

        (with-open [node (tu/->local-node {:node-dir node-dir})]
          (t/is (= last-tx-key
                   (tu/then-await-tx* last-tx-key node (Duration/ofSeconds 2))))

          (t/is (= last-tx-key (tu/latest-completed-tx node))))

        (t/is (zero? (.count (Files/list object-dir))))))))

(t/deftest can-ingest-ts-devices-mini
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-chunk 3000, :rows-per-block 300})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [^ObjectStore os (tu/component node ::os/file-system-object-store)
            ^IMetadataManager mm (tu/component node ::meta/metadata-manager)
            device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     [:put doc])]

        (t/is (= 11000 (count tx-ops)))

        (t/is (nil? (tu/latest-completed-tx node)))

        (let [last-tx-key (reduce
                           (fn [_acc tx-ops]
                             (c2.d/submit-tx node tx-ops))
                           nil
                           (partition-all 100 tx-ops))]

          (t/is (= last-tx-key (tu/then-await-tx* last-tx-key node (Duration/ofSeconds 15))))
          (t/is (= last-tx-key (tu/latest-completed-tx node)))
          (tu/finish-chunk! node)

          (t/is (= {:latest-completed-tx last-tx-key
                    :latest-row-id (dec (count tx-ops))}
                   (-> (meta/latest-chunk-metadata mm)
                       (select-keys [:latest-completed-tx :latest-row-id]))))

          (let [objs (.listObjects os)]
            (t/is (= 4 (count (filter #(re-matches #"^chunk-\p{XDigit}+/temporal\.arrow$" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"temporal-snapshots/\p{XDigit}+.*" %) objs))))
            (t/is (= 1 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-info/metadata\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-readings/metadata\.arrow" %) objs))))
            (t/is (= 1 (count (filter #(re-matches #"chunk-.*/device-info/content-api-version\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"chunk-.*/device-readings/content-battery-level\.arrow" %) objs))))))))

    (t/testing "blocks are row-id aligned"
      (letfn [(row-id-ranges [^String file-name]
                (let [path (-> node-dir (.resolve "objects") (.resolve file-name))
                      json-path (tj/write-arrow-json-file path)]
                  (for [batch (-> (Files/readString json-path)
                                  json/parse-string
                                  (get "batches"))
                        :let [data (-> (get batch "columns")
                                       first
                                       (get "DATA"))]
                        :when (seq data)]

                    [(Long/parseLong (first data))
                     (Long/parseLong (last data))
                     (count data)])))]

        (t/is (= [[0 298 150] [300 598 150] [600 898 150] [900 1198 150]
                  [1200 1498 150] [1500 1798 150] [1800 1998 100]]
                 (row-id-ranges "chunk-00/device-info/content-id.arrow")))

        (t/is (= [[1 299 150] [301 599 150] [601 899 150] [901 1199 150]
                  [1201 1499 150] [1501 1799 150] [1801 2099 200]
                  [2100 2399 300] [2400 2699 300] [2700 2999 300]]
                 (row-id-ranges "chunk-00/device-readings/content-id.arrow")))

        (t/is (= [[0 298 150] [300 598 150] [600 898 150] [900 1198 150]
                  [1200 1498 150] [1500 1798 150] [1800 1998 100]]
                 (row-id-ranges "chunk-00/device-info/content-api-version.arrow")))

        (t/is (= [[1 299 150] [301 599 150] [601 899 150] [901 1199 150]
                  [1201 1499 150] [1501 1799 150] [1801 2099 200]
                  [2100 2399 300] [2400 2699 300] [2700 2999 300]]
                 (row-id-ranges "chunk-00/device-readings/content-battery-level.arrow")))

        (t/is (= [[3000 3299 300] [3300 3599 300] [3600 3899 300] [3900 4199 300]
                  [4200 4499 300] [4500 4799 300] [4800 5099 300]
                  [5100 5399 300] [5400 5699 300] [5700 5999 300]]
                 (row-id-ranges "chunk-2bb8/device-readings/content-id.arrow")))

        (t/is (= [[3000 3299 300] [3300 3599 300] [3600 3899 300] [3900 4199 300]
                  [4200 4499 300] [4500 4799 300] [4800 5099 300]
                  [5100 5399 300] [5400 5699 300] [5700 5999 300]]
                 (row-id-ranges "chunk-2bb8/device-readings/content-battery-level.arrow")))))))

(t/deftest can-ingest-ts-devices-mini-into-multiple-nodes
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini-into-multiple-nodes")
        node-opts {:node-dir node-dir, :rows-per-chunk 1000, :rows-per-block 100}]
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
                     [:put doc])]

        (t/is (= 11000 (count tx-ops)))

        (let [last-tx-key (reduce
                           (fn [_ tx-ops]
                             (c2.d/submit-tx submit-node tx-ops))
                           nil
                           (partition-all 100 tx-ops))]

          (doseq [^Node node (shuffle (take 6 (cycle [node-1 node-2 node-3])))
                  :let [os ^ObjectStore (util/component node ::os/file-system-object-store)]]
            (t/is (= last-tx-key (tu/then-await-tx* last-tx-key node (Duration/ofSeconds 60))))
            (t/is (= last-tx-key (tu/latest-completed-tx node)))

            (Thread/sleep 1000) ;; TODO for now
            (tu/await-temporal-snapshot-build node)

            (let [objs (.listObjects os)]
              (t/is (= 11 (count (filter #(re-matches #"chunk-\p{XDigit}+/temporal\.arrow" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"temporal-snapshots/\p{XDigit}+.arrow" %) objs))))
              (t/is (= 13 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-(?:info|readings)/metadata.arrow" %) objs))))
              (t/is (= 2 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-info/content-api-version\.arrow" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-readings/content-battery-level\.arrow" %) objs)))))))))))

(t/deftest can-ingest-ts-devices-mini-with-stop-start-and-reach-same-state
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini-with-stop-start-and-reach-same-state")
        node-opts {:node-dir node-dir, :rows-per-chunk 1000 :rows-per-block 100}]
    (util/delete-dir node-dir)

    (with-open [submit-node (tu/->local-submit-node {:node-dir node-dir})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     [:put doc])
            [first-half-tx-ops second-half-tx-ops] (split-at (/ (count tx-ops) 2) tx-ops)]

        (t/is (= 5500 (count first-half-tx-ops)))
        (t/is (= 5500 (count second-half-tx-ops)))

        (let [^TransactionInstant
              first-half-tx-key (reduce
                                 (fn [_ tx-ops]
                                   (c2.d/submit-tx submit-node tx-ops))
                                 nil
                                 (partition-all 100 first-half-tx-ops))]

          (with-open [node (tu/->local-node (assoc node-opts :buffers-dir "buffers-1"))]
            (let [^ObjectStore os (util/component node ::os/file-system-object-store)
                  ^InternalIdManager iid-mgr (util/component node :core2.indexer/internal-id-manager)
                  ^IMetadataManager mm (util/component node ::meta/metadata-manager)]
              (t/is (= first-half-tx-key
                       (-> first-half-tx-key
                           (tu/then-await-tx* node (Duration/ofSeconds 10)))))
              (t/is (= first-half-tx-key (tu/latest-completed-tx node)))

              (let [{:keys [^TransactionInstant latest-completed-tx, latest-row-id]}
                    (meta/latest-chunk-metadata mm)]

                (t/is (< (:tx-id latest-completed-tx) (:tx-id first-half-tx-key)))
                (t/is (< latest-row-id (count first-half-tx-ops)))

                (let [objs (.listObjects os)]
                  (t/is (= 5 (count (filter #(re-matches #"^chunk-\p{XDigit}+/temporal\.arrow$" %) objs))))
                  (t/is (= 5 (count (filter #(re-matches #"temporal-snapshots/\p{XDigit}+.*" %) objs))))
                  (t/is (= 2 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-info/metadata\.arrow" %) objs))))
                  (t/is (= 5 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-readings/metadata\.arrow" %) objs))))
                  (t/is (= 2 (count (filter #(re-matches #"chunk-.*/device-info/content-api-version\.arrow" %) objs))))
                  (t/is (= 5 (count (filter #(re-matches #"chunk-.*/device-readings/content-battery-level\.arrow" %) objs)))))

                (t/is (= 2000 (count (.id->internal-id iid-mgr)))))

              (t/is (= :utf8 (.columnType mm "device-readings" "id")))

              (let [^TransactionInstant
                    second-half-tx-key (reduce
                                        (fn [_ tx-ops]
                                          (c2.d/submit-tx submit-node tx-ops))
                                        nil
                                        (partition-all 100 second-half-tx-ops))]

                (t/is (<= (:tx-id first-half-tx-key)
                          (:tx-id (tu/latest-completed-tx node))
                          (:tx-id second-half-tx-key)))

                (with-open [new-node (tu/->local-node (assoc node-opts :buffers-dir "buffers-2"))]
                  (doseq [^Node node [new-node node]
                          :let [^IMetadataManager mm (tu/component node ::meta/metadata-manager)]]

                    (t/is (<= (:tx-id first-half-tx-key)
                              (:tx-id (-> first-half-tx-key
                                          (tu/then-await-tx* node (Duration/ofSeconds 10))))
                              (:tx-id second-half-tx-key)))

                    (t/is (>= (count (.id->internal-id iid-mgr)) 2000))

                    (t/is (= :utf8 (.columnType mm "device-info" "id"))))

                  (doseq [^Node node [new-node node]]
                    (t/is (= second-half-tx-key (-> second-half-tx-key
                                                    (tu/then-await-tx* node (Duration/ofSeconds 15)))))
                    (t/is (= second-half-tx-key (tu/latest-completed-tx node))))

                  (Thread/sleep 1000) ;; TODO for now
                  (tu/await-temporal-snapshot-build node)

                  (doseq [^Node node [new-node node]
                          :let [^ObjectStore os (tu/component node ::os/file-system-object-store)
                                ^IMetadataManager mm (tu/component node ::meta/metadata-manager)]]

                    (let [objs (.listObjects os)]
                      (t/is (= 11 (count (filter #(re-matches #"^chunk-\p{XDigit}+/temporal\.arrow$" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"temporal-snapshots/\p{XDigit}+.*" %) objs))))
                      (t/is (= 2 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-info/metadata\.arrow" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"chunk-\p{XDigit}+/device-readings/metadata\.arrow" %) objs))))
                      (t/is (= 2 (count (filter #(re-matches #"chunk-.*/device-info/content-api-version\.arrow" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"chunk-.*/device-readings/content-battery-level\.arrow" %) objs)))))

                    (t/is (= :utf8 (.columnType mm "device-info" "id")))

                    (t/is (= 2000 (count (.id->internal-id iid-mgr))))))))))))))

(t/deftest merges-column-fields-on-restart
  (let [node-dir (util/->path "target/merges-column-fields")
        node-opts {:node-dir node-dir, :rows-per-chunk 1000, :rows-per-block 100}]
    (util/delete-dir node-dir)

    (with-open [node1 (tu/->local-node (assoc node-opts :buffers-dir "buffers-1"))]
      (let [^IMetadataManager mm1 (tu/component node1 ::meta/metadata-manager)]

        (-> (c2.d/submit-tx node1 [[:put {:id "foo"}]])
            (tu/then-await-tx* node1 (Duration/ofSeconds 1)))

        (tu/finish-chunk! node1)

        (t/is (= :utf8 (.columnType mm1 "xt_docs" "id")))

        (let [tx2 (c2.d/submit-tx node1 [[:put {:id :bar}]
                                         [:put {:id #uuid "8b190984-2196-4144-9fa7-245eb9a82da8"}]
                                         [:put {:id #c2/clj-form :foo}]])]
          (tu/then-await-tx* tx2 node1 (Duration/ofMillis 200))

          (tu/finish-chunk! node1)

          (t/is (= [:union #{:utf8 :keyword :clj-form :uuid}]
                   (.columnType mm1 "xt_docs" "id")))

          (with-open [node2 (tu/->local-node (assoc node-opts :buffers-dir "buffers-1"))]
            (let [^IMetadataManager mm2 (tu/component node2 ::meta/metadata-manager)]
              (tu/then-await-tx* tx2 node2 (Duration/ofMillis 200))

              (t/is (= [:union #{:utf8 :keyword :clj-form :uuid}]
                       (.columnType mm2 "xt_docs" "id"))))))))))

(t/deftest test-await-fails-fast
  (let [e (UnsupportedOperationException. "oh no!")]
    (with-redefs [idx/content-row-copier (fn [& _args]
                                           (throw e))
                  log/log* (let [log* log/log*]
                             (fn [logger level throwable message]
                               (when-not (identical? e throwable)
                                 (log* logger level throwable message))))]
      (with-open [node (node/start-node {})]
        (t/is (thrown-with-msg? Exception #"oh no!"
                                (-> (c2.d/submit-tx node [[:put {:id "foo", :count 42}]])
                                    (tu/then-await-tx* node (Duration/ofSeconds 1)))))))))

(t/deftest test-indexes-sql-insert
  (let [node-dir (util/->path "target/can-index-sql-insert")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir
                                       :clock (tu/->mock-clock)})]
      (let [mm (tu/component node ::meta/metadata-manager)]
        (t/is (nil? (meta/latest-chunk-metadata mm)))

        (let [last-tx-key (c2.api/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")})]
          (t/is (= last-tx-key
                   (c2.sql/submit-tx node [[:sql "INSERT INTO table (id, foo, bar, baz) VALUES (?, ?, ?, ?)"
                                            '[[0, 2, "hello", 12]
                                              [1, 1, "world", 3.3]]]])))

          (t/is (= last-tx-key
                   (tu/then-await-tx* last-tx-key node (Duration/ofSeconds 1)))))

        (tu/finish-chunk! node)

        (tj/check-json (.toPath (io/as-file (io/resource "can-index-sql-insert")))
                       (.resolve node-dir "objects"))))))

(t/deftest test-skips-irrelevant-live-blocks-632
  (with-open [node (node/start-node {:core2/live-chunk {:rows-per-block 2, :rows-per-chunk 10}})]
    (-> (c2.d/submit-tx node [[:put {:name "Håkan", :id :hak}]])
        (tu/then-await-tx* node))

    (tu/finish-chunk! node)

    (c2.d/submit-tx node [[:put {:name "Dan", :id :dan}]
                          [:put {:name "Ivan", :id :iva}]])

    (-> (c2.d/submit-tx node [[:put {:name "James", :id :jms}]
                              [:put {:name "Jon", :id :jon}]])
        (tu/then-await-tx* node))

    (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)
          ^IWatermarkSource wm-src (tu/component node :core2/indexer)]
      (with-open [params (tu/open-params {'?name "Ivan"})]
        (let [gt-literal-selector (expr.meta/->metadata-selector '(> name "Ivan") '#{name} {})
              gt-param-selector (expr.meta/->metadata-selector '(> name ?name) '#{name} params)]

          (t/is (= #{0} (set (keys (.chunksMetadata metadata-mgr)))))

          (letfn [(test-live-blocks [^IWatermark wm, metadata-pred]
                    (with-open [live-blocks (-> (.liveChunk wm)
                                                (.liveTable "xt_docs")
                                                (.liveBlocks #{"name"} metadata-pred))]
                      (let [!res (atom [])]
                        (.forEachRemaining live-blocks
                                           (reify Consumer
                                             (accept [_ in-rels]
                                               (swap! !res conj (-> in-rels (update-vals iv/rel->rows))))))
                        @!res)))]

            (with-open [wm1 (.openWatermark wm-src nil)]
              (t/is (= [{"name" [{:_row-id 1, :name "Dan"} {:_row-id 2, :name "Ivan"}]}
                        {"name" [{:_row-id 3, :name "James"} {:_row-id 4, :name "Jon"}]}]
                       (test-live-blocks wm1 nil))
                    "no selector")

              (t/is (= [{"name" [{:_row-id 3, :name "James"} {:_row-id 4, :name "Jon"}]}]
                       (test-live-blocks wm1 gt-literal-selector))
                    "only second block, literal selector")

              (t/is (= [{"name" [{:_row-id 3, :name "James"} {:_row-id 4, :name "Jon"}]}]
                       (test-live-blocks wm1 gt-param-selector))
                    "only second block, param selector")

              (let [next-tx (-> (c2.d/submit-tx node [[:put {:name "Jeremy", :id :jdt}]])
                                (tu/then-await-tx* node))]

                (with-open [wm2 (.openWatermark wm-src next-tx)]
                  (t/is (= [{"name" [{:_row-id 3, :name "James"} {:_row-id 4, :name "Jon"}]}]
                           (test-live-blocks wm1 gt-literal-selector))
                        "replay with wm1")

                  (t/is (= [{"name" [{:_row-id 3, :name "James"} {:_row-id 4, :name "Jon"}]}
                            {"name" [{:_row-id 5, :name "Jeremy"}]}]
                           (test-live-blocks wm2 gt-literal-selector))
                        "now on wm2"))))))))))
