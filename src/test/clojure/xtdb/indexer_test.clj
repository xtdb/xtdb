(ns xtdb.indexer-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.api.protocols :as xtp]
            [xtdb.indexer :as idx]
            [xtdb.metadata :as meta]
            [xtdb.node :as node]
            [xtdb.object-store :as os]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.ts-devices :as ts]
            [xtdb.types :as types]
            [xtdb.util :as util]
            xtdb.watermark)
  (:import (java.nio.channels ClosedByInterruptException)
           java.nio.file.Files
           java.time.Duration
           [org.apache.arrow.memory BufferAllocator]
           xtdb.IBufferPool
           xtdb.api.protocols.TransactionInstant
           (xtdb.metadata IMetadataManager)
           xtdb.node.Node
           xtdb.object_store.ObjectStore
           (xtdb.watermark IWatermarkSource)))

(t/use-fixtures :once tu/with-allocator)

(def txs
  [[[:put :device-info
     {:xt/id "device-info-demo000000",
      :api-version "23",
      :manufacturer "iobeam",
      :model "pinto",
      :os-name "6.0.1"}]
    [:put :device-readings
     {:xt/id "reading-demo000000",
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
   [[:put :device-info
     {:xt/id "device-info-demo000001",
      :api-version "23",
      :manufacturer "iobeam",
      :model "mustang",
      :os-name "6.0.1"}]
    [:put :device-readings
     {:xt/id "reading-demo000001",
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

(def magic-last-tx-id
  "This value will change if you vary the structure of log entries, such
  as adding new legs to the tx-ops vector, as in memory the tx-id is a byte offset."
  6509)

(t/deftest can-build-chunk-as-arrow-ipc-file-format
  (let [node-dir (util/->path "target/can-build-chunk-as-arrow-ipc-file-format")
        last-tx-key (xtp/map->TransactionInstant {:tx-id magic-last-tx-id, :system-time (util/->instant #inst "2020-01-02")})]
    (util/delete-dir node-dir)

    (util/with-open [node (tu/->local-node {:node-dir node-dir})]
      (let [^BufferAllocator a (tu/component node :xtdb/allocator)
            ^IBufferPool bp (tu/component node :xtdb/buffer-pool)
            mm (tu/component node ::meta/metadata-manager)
            ^IWatermarkSource wm-src (tu/component node :xtdb/indexer)]

        (t/is (nil? (meta/latest-chunk-metadata mm)))

        (t/is (= last-tx-key
                 (last (for [tx-ops txs]
                         (xt/submit-tx node tx-ops)))))

        (t/is (= last-tx-key
                 (tu/then-await-tx last-tx-key node (Duration/ofSeconds 2))))

        (tu/finish-chunk! node)

        (t/is (= {:latest-completed-tx last-tx-key
                  :next-chunk-idx 6}
                 (-> (meta/latest-chunk-metadata mm)
                     (select-keys [:latest-completed-tx :next-chunk-idx]))))

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-chunk-as-arrow-ipc-file-format")))
                       (.resolve node-dir "objects"))

        #_ ; TODO port to buffer pool test that doesn't depend on the structure of the indexer
        (t/testing "buffer pool"
          (let [buffer-name "chunk-00/device_info/metadata.arrow"
                ^ArrowBuf buffer @(.getBuffer bp buffer-name)
                footer (util/read-arrow-footer buffer)]
            (t/is (= 1 (count (.buffers ^BufferPool bp))))
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
              (t/is (= 2 (.getRowCount metadata-batch)))
              (let [id-col-idx (-> (meta/->table-metadata metadata-batch (meta/->table-metadata-idxs metadata-batch))
                                   (.rowIndex "xt$id" -1))]
                (t/is (= "xt$id" (-> (vr/vec->reader (.getVector metadata-batch "columns"))
                                     (.listElementReader)
                                     (.structKeyReader "col-name")
                                     (.getObject id-col-idx))))
                (let [^StructVector cols-data-vec (-> ^ListVector (.getVector metadata-batch "columns")
                                                      (.getDataVector))
                      ^StructVector utf8-type-vec (-> cols-data-vec
                                                      ^StructVector (.getChild "types")
                                                      (.getChild "utf8"))]
                  (t/is (= "device-info-demo000000"
                           (-> (vr/vec->reader (.getChild utf8-type-vec "min"))
                               (.getObject id-col-idx))))
                  (t/is (= "device-info-demo000001"
                           (-> (vr/vec->reader (.getChild utf8-type-vec "max"))
                               (.getObject id-col-idx))))

                  (let [^BigIntVector count-vec (.getChild cols-data-vec "count")]
                    (t/is (= 2 (.get count-vec id-col-idx)))

                    (let [tp (.getTransferPair count-vec a)]
                      (with-open [to (.getTo tp)]
                        (t/is (zero? (.getValueCount to)))
                        (.splitAndTransfer tp 0 12)
                        (t/is  (= (.memoryAddress (.getDataBuffer count-vec))
                                  (.memoryAddress (.getDataBuffer to))))
                        (t/is (= 12 (.getValueCount to)))))))))

            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (let [size (.getSize (.getReferenceManager ^ArrowBuf buffer))]
              (t/is (= size (.getAccountedSize (.getReferenceManager ^ArrowBuf buffer))))
              (.close buffer)
              (t/is (= 1 (.getRefCount (.getReferenceManager ^ArrowBuf buffer)))))))))))

(t/deftest temporal-watermark-is-immutable-567
  (with-open [node (node/start-node {})]
    (let [{tt :system-time} (xt/submit-tx node [[:put :xt_docs {:xt/id :foo, :version 0}]]
                                          {:default-all-valid-time? false})]
      (t/is (= [{:xt/id :foo, :version 0,
                 :xt/valid-from (util/->zdt tt)
                 :xt/valid-to nil
                 :xt/system-from (util/->zdt tt)
                 :xt/system-to nil}]
               (tu/query-ra '[:scan {:table xt_docs}
                              [xt/id version
                               xt/valid-from, xt/valid-to
                               xt/system-from, xt/system-to]]
                            {:node node})))

      (let [{tt2 :system-time} (xt/submit-tx node [[:put :xt_docs {:xt/id :foo, :version 1}]]
                                             {:default-all-valid-time? false})]
        (t/is (= #{{:xt/id :foo, :version 0,
                    :xt/valid-from (util/->zdt tt2)
                    :xt/valid-to nil
                    :xt/system-from (util/->zdt tt)
                    :xt/system-to (util/->zdt tt2)}
                   {:xt/id :foo, :version 0,
                    :xt/valid-from (util/->zdt tt)
                    :xt/valid-to (util/->zdt tt2)
                    :xt/system-from (util/->zdt tt)
                    :xt/system-to nil}
                   {:xt/id :foo, :version 1,
                    :xt/valid-from (util/->zdt tt2)
                    :xt/valid-to nil
                    :xt/system-from (util/->zdt tt2)
                    :xt/system-to nil}}
                 (set (tu/query-ra '[:scan {:table xt_docs, :for-system-time :all-time}
                                     [xt/id version
                                      xt/valid-from, xt/valid-to
                                      xt/system-from, xt/system-to]]
                                   {:node node :default-all-valid-time? true}))))

        #_ ; FIXME #567 this sees the updated xt/system-to of the first entry
        (t/is (= [{:xt/id :foo, :version 0,
                   :xt/valid-from (util/->zdt tt)
                   :xt/valid-to nil
                   :xt/system-from (util/->zdt tt)
                   :xt/system-to nil}]
                 (tu/query-ra '[:scan {:table :xt_docs}
                                [id version
                                 xt/valid-from, xt/valid-to
                                 xt/system-from, xt/system-to]]
                              {:node node, :basis {:tx tx}}))
              "re-using the original snapshot should see the same result")))))

(t/deftest can-handle-dynamic-cols-in-same-block
  (let [node-dir (util/->path "target/can-handle-dynamic-cols-in-same-block")
        tx-ops [[:put :xt_docs {:xt/id "foo"
                                :list [12.0 "foo"]}]
                [:put :xt_docs {:xt/id 24}]
                [:put :xt_docs {:xt/id "bar"
                                :list [#inst "2020-01-01" false]}]
                [:put :xt_docs {:xt/id :baz
                                :struct {:a 1, :b "b"}}]
                [:put :xt_docs {:xt/id 52}]
                [:put :xt_docs {:xt/id :quux
                                :struct {:a true, :c "c"}}]]]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (-> (xt/submit-tx node tx-ops)
          (tu/then-await-tx node (Duration/ofMillis 2000)))

      (tu/finish-chunk! node)

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-handle-dynamic-cols-in-same-block")))
                     (.resolve node-dir "objects")))))

(t/deftest test-multi-block-metadata
  (let [node-dir (util/->path "target/multi-block-metadata")
        tx0 [[:put :xt_docs {:xt/id "foo"
                             :list [12.0 "foo"]}]
             [:put :xt_docs {:xt/id :bar
                             :struct {:a 1, :b "b"}}]
             [:put :xt_docs {:xt/id "baz"
                             :list [#inst "2020-01-01" false]}]
             [:put :xt_docs {:xt/id 24}]]
        tx1 [[:put :xt_docs {:xt/id 52}]
             [:put :xt_docs {:xt/id :quux
                             :struct {:a true, :b {:c "c", :d "d"}}}]]]
    (util/delete-dir node-dir)

    (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 3})]
      (xt/submit-tx node tx0)

      (-> (xt/submit-tx node tx1)
          (tu/then-await-tx node (Duration/ofMillis 200)))

      (tu/finish-chunk! node)

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/multi-block-metadata")))
                     (.resolve node-dir "objects"))

      (let [^IMetadataManager mm (tu/component node ::meta/metadata-manager)]
        (t/is (= (types/col-type->field "xt$id" [:union #{:utf8 :keyword :i64}])
                 (.columnField mm "xt_docs" "xt$id")))

        (t/is (= (types/->field "list" #xt.arrow/type :union false
                                (types/->field "list" #xt.arrow/type :list false
                                               (types/->field "$data$" #xt.arrow/type :union false
                                                              (types/col-type->field :f64)
                                                              (types/col-type->field :utf8)
                                                              (types/col-type->field [:timestamp-tz :micro "UTC"])
                                                              (types/col-type->field :bool)))
                                (types/col-type->field :absent))
                 (.columnField mm "xt_docs" "list")))

        (t/is (= (types/->field "struct" #xt.arrow/type :union false
                                (types/col-type->field :absent)
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "a" #xt.arrow/type :union false
                                                              (types/col-type->field :i64)
                                                              (types/col-type->field :bool)
                                                              )
                                               (types/->field "b" #xt.arrow/type :union false
                                                              (types/col-type->field :utf8)
                                                              (types/->field "struct" #xt.arrow/type :struct false
                                                                             (types/->field "c" #xt.arrow/type :union false
                                                                                            (types/col-type->field :utf8))
                                                                             (types/->field "d" #xt.arrow/type :union false
                                                                                            (types/col-type->field :utf8))))))
                 (.columnField mm "xt_docs" "struct")))))))

(t/deftest round-trips-nils
  (with-open [node (node/start-node {})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id "nil-bar"
                                        :foo "foo"
                                        :bar nil}]
                        [:put :xt_docs {:xt/id "no-bar"
                                        :foo "foo"}]])
    (t/is (= [{:id "nil-bar", :foo "foo", :bar nil}
              {:id "no-bar", :foo "foo"}]
             (xt/q node '{:find [id foo bar]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :xt/id id]
                                  [e :foo foo]
                                  [e :bar bar]]})))))

(t/deftest round-trips-extensions-via-ipc
  (let [node-dir (util/->path "target/round-trips-extensions-via-ipc")
        uuid #uuid "eb965985-de20-4b45-9721-b6a4bbd694bf"]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (-> (xt/submit-tx node [[:put :xt_docs {:xt/id :foo, :uuid uuid}]])
          (tu/then-await-tx node (Duration/ofMillis 2000)))

      (tu/finish-chunk! node)

      (t/is (= #{{:id :foo, :uuid uuid}}
               (set (xt/q node '{:find [id uuid]
                                 :where [(match :xt_docs {:xt/id id})
                                         [id :uuid uuid]]})))))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (t/is (= #{{:id :foo, :uuid uuid}}
               (set (xt/q node '{:find [id uuid]
                                 :where [(match :xt_docs {:xt/id id})
                                         [id :uuid uuid]]})))))))

(t/deftest writes-log-file
  (let [node-dir (util/->path "target/writes-log-file")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/submit-tx node [[:put :xt_docs {:xt/id "foo"}]
                          [:put :xt_docs {:xt/id "bar"}]])

      ;; aborted tx shows up in log
      (xt/submit-tx node [[:sql "INSERT INTO foo (xt$id, xt$valid_from, xt$valid_to) VALUES (1, DATE '2020-01-01', DATE '2019-01-01')"]])

      (-> (xt/submit-tx node [[:delete :xt_docs "foo" {:for-valid-time [:in #inst "2020-04-01"]}]
                              [:put :xt_docs {:xt/id "bar", :month "april"},
                               {:for-valid-time [:in #inst "2020-04-01" #inst "2020-05-01"]}]])
          (tu/then-await-tx node))

      (tu/finish-chunk! node)

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/writes-log-file")))
                     (.resolve node-dir "objects")))))

(t/deftest can-stop-node-without-writing-chunks
  (let [node-dir (util/->path "target/can-stop-node-without-writing-chunks")
        last-tx-key (xtp/map->TransactionInstant {:tx-id magic-last-tx-id, :system-time (util/->instant #inst "2020-01-02")})]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (let [object-dir (.resolve node-dir "objects")]

        (util/mkdirs object-dir)

        (t/is (= last-tx-key
                 (last (for [tx-ops txs]
                         (xt/submit-tx node tx-ops)))))

        (t/is (= last-tx-key
                 (tu/then-await-tx last-tx-key node (Duration/ofSeconds 2))))
        (t/is (= last-tx-key (tu/latest-completed-tx node)))

        (with-open [node (tu/->local-node {:node-dir node-dir})]
          (t/is (= last-tx-key
                   (tu/then-await-tx last-tx-key node (Duration/ofSeconds 2))))

          (t/is (= last-tx-key (tu/latest-completed-tx node))))

        (t/is (zero? (.count (Files/list object-dir))))))))

(t/deftest can-ingest-ts-devices-mini
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-chunk 3000, :rows-per-block 300})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [^IBufferPool bp (tu/component node :xtdb/buffer-pool)
            ^IMetadataManager mm (tu/component node ::meta/metadata-manager)
            device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     [:put (:table (meta doc)) doc])]

        (t/is (= 11000 (count tx-ops)))

        (t/is (nil? (tu/latest-completed-tx node)))

        (let [last-tx-key (reduce
                           (fn [_acc tx-ops]
                             (xt/submit-tx node tx-ops))
                           nil
                           (partition-all 100 tx-ops))]

          (t/is (= last-tx-key (tu/then-await-tx last-tx-key node (Duration/ofSeconds 15))))
          (t/is (= last-tx-key (tu/latest-completed-tx node)))
          (tu/finish-chunk! node)

          (t/is (= {:latest-completed-tx last-tx-key
                    :next-chunk-idx 11110}
                   (-> (meta/latest-chunk-metadata mm)
                       (select-keys [:latest-completed-tx :next-chunk-idx]))))

          (let [objs (.listObjects bp)]
            (t/is (= 4 (count (filter #(re-matches #"chunk-metadata/\p{XDigit}+\.transit.json" %) objs))))
            (t/is (= 2 (count (filter #(re-matches #"tables/device_info/(.+?)/.+\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"tables/device_readings/data/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"tables/device_readings/meta/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"tables/xt\$txs/data/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"tables/xt\$txs/meta/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))))))))

(t/deftest can-ingest-ts-devices-mini-into-multiple-nodes
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini-into-multiple-nodes")
        node-opts {:node-dir node-dir, :rows-per-chunk 1000, :rows-per-block 100}]
    (util/delete-dir node-dir)

    (with-open [node-1 (tu/->local-node (assoc node-opts :buffers-dir "objects-1"))
                node-2 (tu/->local-node (assoc node-opts :buffers-dir "objects-2"))
                node-3 (tu/->local-node (assoc node-opts :buffers-dir "objects-3"))
                submit-node (tu/->local-submit-node {:node-dir node-dir})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     [:put (:table (meta doc)) doc])]

        (t/is (= 11000 (count tx-ops)))

        (let [last-tx-key (reduce
                           (fn [_ tx-ops]
                             (xt/submit-tx submit-node tx-ops))
                           nil
                           (partition-all 100 tx-ops))]

          (doseq [^Node node (shuffle (take 6 (cycle [node-1 node-2 node-3])))
                  :let [^IBufferPool bp (util/component node :xtdb/buffer-pool)
                        ^IMetadataManager mm (util/component node ::meta/metadata-manager)]]
            (t/is (= last-tx-key (tu/then-await-tx last-tx-key node (Duration/ofSeconds 60))))
            (t/is (= last-tx-key (tu/latest-completed-tx node)))

            (let [objs (.listObjects bp)]
              (t/is (= 11 (count (filter #(re-matches #"chunk-metadata/\p{XDigit}+\.transit.json" %) objs))))
              (t/is (= 4 (count (filter #(re-matches #"tables/device_info/(.+?)/.+\.arrow" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"tables/device_readings/data/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"tables/device_readings/meta/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"tables/xt\$txs/data/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
              (t/is (= 11 (count (filter #(re-matches #"tables/xt\$txs/meta/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs)))))
            (t/is (= :utf8 (types/field->col-type (.columnField mm "device_readings" "xt$id"))))))))))

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
                     [:put (:table (meta doc)) doc])
            [first-half-tx-ops second-half-tx-ops] (split-at (/ (count tx-ops) 2) tx-ops)]

        (t/is (= 5500 (count first-half-tx-ops)))
        (t/is (= 5500 (count second-half-tx-ops)))

        (let [^TransactionInstant
              first-half-tx-key (reduce
                                 (fn [_ tx-ops]
                                   (xt/submit-tx submit-node tx-ops))
                                 nil
                                 (partition-all 100 first-half-tx-ops))]

          (with-open [node (tu/->local-node (assoc node-opts :buffers-dir "objects-1"))]
            (let [^IBufferPool bp (util/component node :xtdb/buffer-pool)
                  ^IMetadataManager mm (util/component node ::meta/metadata-manager)]
              (t/is (= first-half-tx-key
                       (-> first-half-tx-key
                           (tu/then-await-tx node (Duration/ofSeconds 10)))))
              (t/is (= first-half-tx-key (tu/latest-completed-tx node)))

              (let [{:keys [^TransactionInstant latest-completed-tx, next-chunk-idx]}
                    (meta/latest-chunk-metadata mm)]

                (t/is (< (:tx-id latest-completed-tx) (:tx-id first-half-tx-key)))
                (t/is (< next-chunk-idx (count first-half-tx-ops)))

                (let [objs (.listObjects bp)]
                  (t/is (= 5 (count (filter #(re-matches #"chunk-metadata/\p{XDigit}+\.transit.json" %) objs))))
                  (t/is (= 4 (count (filter #(re-matches #"tables/device_info/(.+?)/.+\.arrow" %) objs))))
                  (t/is (= 5 (count (filter #(re-matches #"tables/device_readings/data/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
                  (t/is (= 5 (count (filter #(re-matches #"tables/device_readings/meta/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
                  (t/is (= 5 (count (filter #(re-matches #"tables/xt\$txs/data/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
                  (t/is (= 5 (count (filter #(re-matches #"tables/xt\$txs/meta/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))))

              (t/is (= :utf8
                       (types/field->col-type (.columnField mm "device_readings" "xt$id"))))

              (let [^TransactionInstant
                    second-half-tx-key (reduce
                                        (fn [_ tx-ops]
                                          (xt/submit-tx submit-node tx-ops))
                                        nil
                                        (partition-all 100 second-half-tx-ops))]

                (t/is (<= (:tx-id first-half-tx-key)
                          (:tx-id (tu/latest-completed-tx node))
                          (:tx-id second-half-tx-key)))

                (with-open [new-node (tu/->local-node (assoc node-opts :buffers-dir "objects-2"))]
                  (doseq [^Node node [new-node node]
                          :let [^IMetadataManager mm (tu/component node ::meta/metadata-manager)]]

                    (t/is (<= (:tx-id first-half-tx-key)
                              (:tx-id (-> first-half-tx-key
                                          (tu/then-await-tx node (Duration/ofSeconds 10))))
                              (:tx-id second-half-tx-key)))

                    (t/is (= :utf8
                             (types/field->col-type (.columnField mm "device_info" "xt$id")))))

                  (doseq [^Node node [new-node node]]
                    (t/is (= second-half-tx-key (-> second-half-tx-key
                                                    (tu/then-await-tx node (Duration/ofSeconds 15)))))
                    (t/is (= second-half-tx-key (tu/latest-completed-tx node))))

                  (doseq [^Node node [new-node node]
                          :let [^IBufferPool bp (tu/component node :xtdb/buffer-pool)
                                ^IMetadataManager mm (tu/component node ::meta/metadata-manager)]]

                    (let [objs (.listObjects bp)]
                      (t/is (= 11 (count (filter #(re-matches #"chunk-metadata/\p{XDigit}+\.transit.json" %) objs))))
                      (t/is (= 4 (count (filter #(re-matches #"tables/device_info/(.+?)/.+\.arrow" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"tables/device_readings/data/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"tables/device_readings/meta/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"tables/xt\$txs/data/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs))))
                      (t/is (= 11 (count (filter #(re-matches #"tables/xt\$txs/meta/log-l\p{XDigit}+-rf\p{XDigit}+-nr\p{XDigit}+\.arrow" %) objs)))))

                    (t/is (= :utf8
                             (types/field->col-type (.columnField mm "device_info" "xt$id"))))))))))))))

(t/deftest merges-column-fields-on-restart
  (let [node-dir (util/->path "target/merges-column-fields")
        node-opts {:node-dir node-dir, :rows-per-chunk 1000, :rows-per-block 100}]
    (util/delete-dir node-dir)

    (with-open [node1 (tu/->local-node (assoc node-opts :buffers-dir "objects-1"))]
      (let [^IMetadataManager mm1 (tu/component node1 ::meta/metadata-manager)]

        (-> (xt/submit-tx node1 [[:put :xt_docs {:xt/id 0, :v "foo"}]])
            (tu/then-await-tx node1 (Duration/ofSeconds 1)))

        (tu/finish-chunk! node1)

        (t/is (=  :utf8
                  (types/field->col-type (.columnField mm1 "xt_docs" "v"))))

        (let [tx2 (xt/submit-tx node1 [[:put :xt_docs {:xt/id 1, :v :bar}]
                                       [:put :xt_docs {:xt/id 2, :v #uuid "8b190984-2196-4144-9fa7-245eb9a82da8"}]
                                       [:put :xt_docs {:xt/id 3, :v #xt/clj-form :foo}]])]
          (tu/then-await-tx tx2 node1 (Duration/ofMillis 200))

          (tu/finish-chunk! node1)

          (t/is (= [:union #{:utf8 :clj-form :keyword :uuid}]
                   (types/field->col-type (.columnField mm1 "xt_docs" "v"))))

          (with-open [node2 (tu/->local-node (assoc node-opts :buffers-dir "objects-1"))]
            (let [^IMetadataManager mm2 (tu/component node2 ::meta/metadata-manager)]
              (tu/then-await-tx tx2 node2 (Duration/ofMillis 200))

              (t/is (= [:union #{:utf8 :clj-form :keyword :uuid}]
                       (types/field->col-type (.columnField mm2 "xt_docs" "v")))))))))))

(t/deftest test-await-fails-fast
  (let [e (UnsupportedOperationException. "oh no!")]
    (with-redefs [idx/->put-indexer (fn [& _args]
                                      (throw e))
                  log/log* (let [log* log/log*]
                             (fn [logger level throwable message]
                               (when-not (identical? e throwable)
                                 (log* logger level throwable message))))]
      (with-open [node (node/start-node {})]
        (t/is (thrown-with-msg? Exception #"oh no!"
                                (-> (xt/submit-tx node [[:put :xt_docs {:xt/id "foo", :count 42}]])
                                    (tu/then-await-tx node (Duration/ofSeconds 1)))))))))

(t/deftest bug-catch-closed-by-interrupt-exception-740
  (let [e (ClosedByInterruptException.)]
    (with-redefs [idx/->sql-indexer (fn [& _args]
                                      (throw e))
                  log/log* (let [log* log/log*]
                             (fn [logger level throwable message]
                               (when-not (identical? e throwable)
                                 (log* logger level throwable message))))]
      (with-open [node (node/start-node {})]
        (t/is (thrown-with-msg? Exception #"ClosedByInterruptException"
                                (-> (xt/submit-tx node [[:sql "INSERT INTO foo(xt$id) VALUES (1)"]])
                                    (tu/then-await-tx node (Duration/ofSeconds 1)))))))))

(t/deftest test-indexes-sql-insert
  (let [node-dir (util/->path "target/can-index-sql-insert")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir
                                       :clock (tu/->mock-clock)})]
      (let [mm (tu/component node ::meta/metadata-manager)]
        (t/is (nil? (meta/latest-chunk-metadata mm)))

        (let [last-tx-key (xtp/map->TransactionInstant {:tx-id 0, :system-time (util/->instant #inst "2020-01-01")})]
          (t/is (= last-tx-key
                   (xt/submit-tx node [[:sql-batch ["INSERT INTO table (xt$id, foo, bar, baz) VALUES (?, ?, ?, ?)"
                                                    [0, 2, "hello", 12]
                                                    [1, 1, "world", 3.3]]]])))

          (t/is (= last-tx-key
                   (tu/then-await-tx last-tx-key node (Duration/ofSeconds 1)))))

        (tu/finish-chunk! node)

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-index-sql-insert")))
                       (.resolve node-dir "objects"))))))
