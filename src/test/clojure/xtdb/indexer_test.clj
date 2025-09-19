(ns xtdb.indexer-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.basis :as basis]
            [xtdb.check-pbuf :as cpb]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.indexer :as idx]
            [xtdb.log :as xt-log]
            [xtdb.object-store :as os]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.ts-devices :as ts]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import java.nio.ByteBuffer
           (java.nio.channels ClosedByInterruptException)
           java.nio.file.Files
           (java.time Duration Instant InstantSource ZoneId)
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector.types UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType$Union]
           (xtdb.arrow NullVector Relation)
           xtdb.trie.MemoryHashTrie))

(t/use-fixtures :once tu/with-allocator)
(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(def txs
  [[[:put-docs :device-info
     {:xt/id "device-info-demo000000",
      :api-version "23",
      :manufacturer "iobeam",
      :model "pinto",
      :os-name "6.0.1"}]
    [:put-docs :device-readings
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
   [[:put-docs :device-info
     {:xt/id "device-info-demo000001",
      :api-version "23",
      :manufacturer "iobeam",
      :model "mustang",
      :os-name "6.0.1"}]
    [:put-docs :device-readings
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
  4769)

(t/deftest can-build-block-as-arrow-ipc-file-format
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/can-build-block-as-arrow-ipc-file-format")
          last-tx-key (serde/->TxKey magic-last-tx-id (time/->instant #inst "2020-01-02"))]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (let [db (db/primary-db node)
              block-cat (.getBlockCatalog db)]
          (t/is (nil? (.getCurrentBlockIndex block-cat)))

          (t/is (= {:tx-id magic-last-tx-id}
                   (last (for [tx-ops txs]
                           (xt/submit-tx node tx-ops {:default-tz #xt/zone "Europe/London"})))))

          (xt-log/sync-node node (Duration/ofSeconds 2))

          (tu/flush-block! node)

          (t/is (= last-tx-key (.getLatestCompletedTx block-cat)))

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-block-as-arrow-ipc-file-format")))
                         (.resolve node-dir "objects"))

          (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-block-as-arrow-ipc-file-format")))
                          (.resolve node-dir "objects")))))))

(t/deftest temporal-watermark-is-immutable-2354
  (let [tx (xt/execute-tx tu/*node* [[:put-docs :xt_docs {:xt/id :foo, :version 0}]])
        tt (.getSystemTime tx)]
    (t/is (= [{:xt/id :foo, :version 0,
               :xt/valid-from (time/->zdt tt)
               :xt/system-from (time/->zdt tt)}]
             (tu/query-ra '[:scan {:table #xt/table xt_docs}
                            [_id version
                             _valid_from, _valid_to
                             _system_from, _system_to]]
                          {:node tu/*node*})))

    (let [tx1 (xt/execute-tx tu/*node* [[:put-docs :xt_docs {:xt/id :foo, :version 1}]])
          tt2 (.getSystemTime tx1)]
      (t/is (= #{{:xt/id :foo, :version 0,
                  :xt/valid-from (time/->zdt tt2)
                  :xt/system-from (time/->zdt tt)
                  :xt/system-to (time/->zdt tt2)}
                 {:xt/id :foo, :version 0,
                  :xt/valid-from (time/->zdt tt)
                  :xt/valid-to (time/->zdt tt2)
                  :xt/system-from (time/->zdt tt)}
                 {:xt/id :foo, :version 1,
                  :xt/valid-from (time/->zdt tt2)
                  :xt/system-from (time/->zdt tt2)}}
               (set (tu/query-ra '[:scan {:table #xt/table xt_docs,
                                          :for-system-time :all-time,
                                          :for-valid-time :all-time}
                                   [_id version
                                    _valid_from, _valid_to
                                    _system_from, _system_to]]
                                 {:node tu/*node*}))))

      (t/is (= [{:xt/id :foo, :version 0,
                 :xt/valid-from (time/->zdt tt)
                 :xt/system-from (time/->zdt tt)}]
               (tu/query-ra '[:scan {:table #xt/table xt_docs}
                              [_id version
                               _valid_from, _valid_to
                               _system_from, _system_to]]
                            {:node tu/*node*,
                             :snapshot-token (basis/->time-basis-str {"xtdb" [tt]})}))
            "re-using the original tx basis should see the same result"))))

(t/deftest can-handle-dynamic-cols-in-same-block
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/can-handle-dynamic-cols-in-same-block")
          tx-ops [[:put-docs :xt_docs {:xt/id "foo"
                                       :list [12.0 "foo"]}]
                  [:put-docs :xt_docs {:xt/id 24}]
                  [:put-docs :xt_docs {:xt/id "bar"
                                       :list [#inst "2020-01-01" false]}]
                  [:put-docs :xt_docs {:xt/id :baz
                                       :struct {:a 1, :b "b"}}]
                  [:put-docs :xt_docs {:xt/id 52}]
                  [:put-docs :xt_docs {:xt/id :quux
                                       :struct {:a true, :c "c"}}]]]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir, :storage-epoch 1})]
        (xt/execute-tx node tx-ops)

        (tu/flush-block! node)

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-handle-dynamic-cols-in-same-block")))
                       (.resolve node-dir "objects"))

        (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-handle-dynamic-cols-in-same-block")))
                        (.resolve node-dir "objects"))))))

(t/deftest test-compacted-trie-details
  (binding [c/*ignore-signal-block?* true]
    (let [expected-path (.toPath (io/as-file (io/resource "xtdb/indexer-test/compacted-trie-details")))
          node-dir (util/->path "target/compacted-trie-details")]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (xt/execute-tx node [[:put-docs :foo {:xt/id 1, :a "hello"} {:xt/id 2, :a "world"}]]
                       {:default-tz #xt/zone "Europe/London"})
        (tu/flush-block! node)
        (c/compact-all! node #xt/duration "PT1S")

        (cpb/check-pbuf expected-path (.resolve node-dir "objects") {:file-pattern #"^b00.binpb.*"})

        (xt/execute-tx node [[:put-docs :foo {:xt/id 3, :a "foo"} {:xt/id 4, :a "bar"}]]
                       {:default-tz #xt/zone "Europe/London"})
        (tu/flush-block! node)
        (c/compact-all! node #xt/duration "PT1S")

        (cpb/check-pbuf expected-path (.resolve node-dir "objects"))))))

;; TODO misnomer: this should be multi-page-metadata
(t/deftest test-multi-block-metadata
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/multi-block-metadata")
          tx0 [[:put-docs :xt_docs {:xt/id "foo"
                                    :list [12.0 "foo"]}]
               [:put-docs :xt_docs {:xt/id :bar
                                    :struct {:a 1, :b "b"}}]
               [:put-docs :xt_docs {:xt/id "baz"
                                    :list [#inst "2020-01-01" false]}]
               [:put-docs :xt_docs {:xt/id 24}]]
          tx1 [[:put-docs :xt_docs {:xt/id 52}]
               [:put-docs :xt_docs {:xt/id :quux
                                    :struct {:a true, :b {:c "c", :d "d"}}}]]]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-page 3})]
        (xt/execute-tx node tx0 {:default-tz #xt/zone "Europe/London"})

        (xt/execute-tx node tx1 {:default-tz #xt/zone "Europe/London"})

        (tu/flush-block! node)

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/multi-block-metadata")))
                       (.resolve node-dir "objects"))

        (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/indexer-test/multi-block-metadata")))
                        (.resolve node-dir "objects"))

        (let [db (db/primary-db node)
              tc (.getTableCatalog db)]
          (t/is (= #xt/field ["_id" :union ["utf8" :utf8] ["keyword" :keyword] ["i64" :i64]]
                   (.getField tc #xt/table xt_docs "_id")))

          (t/is (= #xt/field ["list" :list :?
                              ["$data$" :union
                               ["f64" :f64]
                               ["utf8" :utf8]
                               ["timestamp-tz-micro-utc" [:timestamp-tz :micro "UTC"]]
                               ["bool" :bool]]]
                   (.getField tc #xt/table xt_docs "list")))

          (t/is (= #xt/field ["struct" :struct :?
                              ["a" :union
                               ["i64" :i64]
                               ["bool" :bool]]
                              ["b" :union
                               ["utf8" :utf8]
                               ["struct" :struct ["c" :utf8] ["d" :utf8]]]]
                   (.getField tc #xt/table xt_docs "struct"))))))))

(t/deftest drops-nils-on-round-trip
  (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:xt/id "nil-bar", :foo "foo", :bar nil}]
                           [:put-docs :xt_docs {:xt/id "no-bar", :foo "foo"}]])
  (t/is (= [{:id "nil-bar", :foo "foo"}
            {:id "no-bar", :foo "foo"}]
           (xt/q tu/*node* '(from :xt_docs [{:xt/id id} foo bar])))))

(t/deftest round-trips-extensions-via-ipc
  (let [node-dir (util/->path "target/round-trips-extensions-via-ipc")
        uuid #uuid "eb965985-de20-4b45-9721-b6a4bbd694bf"]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo, :uuid uuid}]])

      (tu/flush-block! node)

      (t/is (= #{{:id :foo, :uuid uuid}}
               (set (xt/q node '(from :xt_docs [{:xt/id id} uuid]))))))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (t/is (= #{{:id :foo, :uuid uuid}}
               (set (xt/q node '(from :xt_docs [{:xt/id id} uuid]))))))))

(t/deftest writes-log-file
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/writes-log-file")]
      (util/delete-dir node-dir)

      (with-open [node (tu/->local-node {:node-dir node-dir})]
        (xt/submit-tx node [[:put-docs :xt_docs {:xt/id "foo"}]
                            [:put-docs :xt_docs {:xt/id "bar"}]]
                      {:default-tz #xt/zone "Europe/London"})

        ;; aborted tx shows up in log
        (t/is (anomalous? [:incorrect :xtdb.indexer/invalid-valid-times]
                          (xt/execute-tx node [[:sql "INSERT INTO foo (_id, _valid_from, _valid_to) VALUES (1, DATE '2020-01-01', DATE '2019-01-01')"]]
                                         {:default-tz #xt/zone "Europe/London"})))

        (xt/execute-tx node [[:delete-docs {:from :xt_docs, :valid-from #inst "2020-04-01"} "foo"]
                             [:put-docs {:into :xt_docs, :valid-from #inst "2020-04-01", :valid-to #inst "2020-05-01"}
                              {:xt/id "bar", :month "april"}]]
                       {:default-tz #xt/zone "Europe/London"})

        (t/is (= [{:xt/id "bar",
                   :xt/valid-from #xt/zdt "2020-01-01T00:00Z[UTC]",
                   :xt/valid-to #xt/zdt "2020-04-01T00:00Z[UTC]"}
                  {:xt/id "bar", :month "april",
                   :xt/valid-from #xt/zdt "2020-04-01T00:00Z[UTC]",
                   :xt/valid-to #xt/zdt "2020-05-01T00:00Z[UTC]"}
                  {:xt/id "bar", :xt/valid-from #xt/zdt "2020-05-01T00:00Z[UTC]"}
                  {:xt/id "foo",
                   :xt/valid-from #xt/zdt "2020-01-01T00:00Z[UTC]",
                   :xt/valid-to #xt/zdt "2020-04-01T00:00Z[UTC]"}]
                 (xt/q node "SELECT *, _valid_from, _valid_to FROM xt_docs FOR ALL VALID_TIME ORDER BY _id, _valid_from")))

        (tu/flush-block! node)

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/writes-log-file")))
                       (.resolve node-dir "objects"))

        (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/indexer-test/writes-log-file")))
                        (.resolve node-dir "objects"))))))

(t/deftest can-stop-node-without-writing-blocks
  (let [node-dir (util/->path "target/can-stop-node-without-writing-blocks")
        last-tx-key (serde/->TxKey magic-last-tx-id (time/->instant #inst "2020-01-02"))]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (let [object-dir (.resolve node-dir "objects")]

        (util/mkdirs object-dir)

        (t/is (= {:tx-id magic-last-tx-id}
                 (last (for [tx-ops txs]
                         (xt/submit-tx node tx-ops
                                       {:default-tz #xt/zone "Europe/London"})))))

        (xt-log/sync-node node (Duration/ofSeconds 2))
        (t/is (= {"xtdb" [last-tx-key]} (xtp/latest-completed-txs node)))

        (with-open [node (tu/->local-node {:node-dir node-dir})]
          (xt-log/sync-node node (Duration/ofSeconds 2))
          (t/is (= {"xtdb" [last-tx-key]}
                   (xtp/latest-completed-txs node)))

          (t/is (= {"xtdb" [last-tx-key]} (xtp/latest-completed-txs node))))

        (t/is (zero? (->> (.toList (Files/list object-dir))
                          (filter util/is-file?)
                          count)))))))

(t/deftest can-ingest-ts-devices-mini
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 3000, :rows-per-page 300, :compactor-threads 0})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [db (db/primary-db node)
            bp (.getBufferPool db)
            block-cat (.getBlockCatalog db)
            device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     [:put-docs (:table (meta doc)) doc])]

        (t/is (= 11000 (count tx-ops)))

        (t/is (= {"xtdb" [nil]} (xtp/latest-completed-txs node)))

        (let [{last-tx-id :tx-id} (reduce
                                   (fn [_acc tx-ops]
                                     (xt/submit-tx node tx-ops))
                                   nil
                                   (partition-all 100 tx-ops))
              last-tx-key (serde/->TxKey last-tx-id (time/->instant #inst "2020-04-19"))]

          (xt-log/sync-node node (Duration/ofSeconds 15))
          (t/is (= {"xtdb" [last-tx-key]} (xtp/latest-completed-txs node)))
          (tu/flush-block! node)

          (t/is (= last-tx-key (.getLatestCompletedTx block-cat)))

          (let [objs (mapv (comp str :key os/<-StoredObject) (.listAllObjects bp))]
            (t/is (= 4 (count (filter #(re-matches #"blocks/b\p{XDigit}+\.binpb" %) objs))))
            (t/is (= 2 (count (filter #(re-matches #"tables/public\$device_info/(.+?)/l00.+\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"tables/public\$device_readings/data/l00.+?\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"tables/public\$device_readings/meta/l00.+?\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"tables/xt\$txs/data/l00.+?\.arrow" %) objs))))
            (t/is (= 4 (count (filter #(re-matches #"tables/xt\$txs/meta/l00.+?\.arrow" %) objs))))))))))

(t/deftest can-ingest-ts-devices-mini-with-stop-start-and-reach-same-state
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini-with-stop-start-and-reach-same-state")
        node-opts {:node-dir node-dir, :rows-per-block 1000 :rows-per-page 100
                   :instant-src (InstantSource/system)
                   :compactor-threads 0}]
    (util/delete-dir node-dir)

    (util/with-close-on-catch [node1 (tu/->local-node node-opts)]
      (with-open [info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                  readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
        (let [device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
              readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
              [initial-readings rest-readings] (split-at (count device-infos) readings)
              tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                       [:put-docs (:table (meta doc)) doc])
              [first-half-tx-ops second-half-tx-ops] (split-at (/ (count tx-ops) 2) tx-ops)]

          (t/is (= 5500 (count first-half-tx-ops)))
          (t/is (= 5500 (count second-half-tx-ops)))

          (let [{first-half-tx-id :tx-id} (reduce
                                           (fn [_ tx-ops]
                                             (xt/submit-tx node1 tx-ops))
                                           nil
                                           (partition-all 100 first-half-tx-ops))
                first-half-await-token (xtp/await-token node1)]
            (.close node1)

            (util/with-close-on-catch [node2 (tu/->local-node (assoc node-opts :buffers-dir "objects-1"))]
              (let [db (db/primary-db node2)
                    bp (.getBufferPool db)
                    block-cat (.getBlockCatalog db)
                    tc (.getTableCatalog db)]

                (xt-log/await-node node2 first-half-await-token (Duration/ofSeconds 10))
                (t/is (= first-half-tx-id
                         (-> (xtp/latest-completed-txs node2) (get-in ["xtdb" 0 :tx-id]))))

                (let [latest-completed-tx (.getLatestCompletedTx block-cat)]

                  (t/is (< (:tx-id latest-completed-tx) first-half-tx-id))

                  (Thread/sleep 250)    ; wait for the block to finish writing to disk
                                        ; we don't have an accessible hook for this, beyond awaiting the tx
                  (let [objs (mapv (comp str :key os/<-StoredObject) (.listAllObjects bp))]
                    (t/is (= 5 (count (filter #(re-matches #"blocks/b\p{XDigit}+\.binpb" %) objs))))
                    (t/is (= 4 (count (filter #(re-matches #"tables/public\$device_info/(.+?)/l00.+\.arrow" %) objs))))
                    (t/is (= 5 (count (filter #(re-matches #"tables/public\$device_readings/data/l00.+?\.arrow" %) objs))))
                    (t/is (= 5 (count (filter #(re-matches #"tables/public\$device_readings/meta/l00.+?\.arrow" %) objs))))
                    (t/is (= 5 (count (filter #(re-matches #"tables/xt\$txs/data/l00.+?\.arrow" %) objs))))
                    (t/is (= 5 (count (filter #(re-matches #"tables/xt\$txs/meta/l00.+?\.arrow" %) objs))))))

                (t/is (= :utf8
                         (types/field->col-type (.getField tc #xt/table device_readings "_id"))))

                (let [{second-half-tx-id :tx-id} (reduce
                                                  (fn [_ tx-ops]
                                                    (xt/submit-tx node2 tx-ops))
                                                  nil
                                                  (partition-all 100 second-half-tx-ops))
                      second-half-await-token (xtp/await-token node2)]

                  (t/is (<= first-half-tx-id
                            (get-in (xtp/latest-completed-txs node2) ["xtdb" 0 :tx-id])
                            second-half-tx-id))

                  (.close node2)

                  (with-open [node3 (tu/->local-node (assoc node-opts :buffers-dir "objects-2"))]
                    (let [db (db/primary-db node3)
                          bp (.getBufferPool db)]
                      (xt-log/await-node node3 first-half-await-token (Duration/ofSeconds 10))
                      (t/is (<= first-half-tx-id
                                (-> (xtp/latest-completed-txs node3) (get-in ["xtdb" 0 :tx-id]))
                                second-half-tx-id))

                      (t/is (= :utf8
                               (types/field->col-type (.getField tc #xt/table device_info "_id"))))

                      (xt-log/await-node node3 second-half-await-token (Duration/ofSeconds 15))
                      (t/is (= second-half-tx-id (-> (xtp/latest-completed-txs node3) (get-in ["xtdb" 0 :tx-id]))))

                      (Thread/sleep 250); wait for the block to finish writing to disk
                                        ; we don't have an accessible hook for this, beyond awaiting the tx
                      (let [objs (mapv (comp str :key os/<-StoredObject) (.listAllObjects bp))]
                        (t/is (= 11 (count (filter #(re-matches #"blocks/b\p{XDigit}+\.binpb" %) objs))))
                        (t/is (= 4 (count (filter #(re-matches #"tables/public\$device_info/(.+?)/l00-.+.arrow" %) objs))))
                        (t/is (= 11 (count (filter #(re-matches #"tables/public\$device_readings/data/l00-.+.arrow" %) objs))))
                        (t/is (= 11 (count (filter #(re-matches #"tables/public\$device_readings/meta/l00-.+.arrow" %) objs))))
                        (t/is (= 11 (count (filter #(re-matches #"tables/xt\$txs/data/l00-.+.arrow" %) objs))))
                        (t/is (= 11 (count (filter #(re-matches #"tables/xt\$txs/meta/l00-.+.arrow" %) objs)))))

                      (t/is (= :utf8
                               (types/field->col-type (.getField tc #xt/table device_info "_id")))))))))))))))

(t/deftest merges-column-fields-on-restart
  (let [node-dir (util/->path "target/merges-column-fields")
        node-opts {:node-dir node-dir, :rows-per-block 1000, :rows-per-page 100}]
    (util/delete-dir node-dir)

    (with-open [node1 (tu/->local-node (assoc node-opts :buffers-dir "objects-1"))]
      (let [tc1 (.getTableCatalog (db/primary-db node1))]

        (xt/execute-tx node1 [[:put-docs :xt_docs {:xt/id 0, :v "foo"}]])

        (tu/flush-block! node1)

        (t/is (= :utf8
                 (types/field->col-type (.getField tc1 #xt/table xt_docs "v"))))

        (xt/execute-tx node1 [[:put-docs :xt_docs {:xt/id 1, :v :bar}]
                              [:put-docs :xt_docs {:xt/id 2, :v #uuid "8b190984-2196-4144-9fa7-245eb9a82da8"}]
                              [:put-docs :xt_docs {:xt/id 3, :v #xt/clj-form :foo}]])

        (tu/flush-block! node1)

        (t/is (= [:union #{:utf8 :transit :keyword :uuid}]
                 (types/field->col-type (.getField tc1 #xt/table xt_docs "v"))))))

    (with-open [node2 (tu/->local-node (assoc node-opts :buffers-dir "objects-1"))]
      (let [tc2 (.getTableCatalog (db/primary-db node2))]
        (xt-log/sync-node node2 (Duration/ofMillis 200))

        (t/is (= [:union #{:utf8 :transit :keyword :uuid}]
                 (types/field->col-type (.getField tc2 #xt/table xt_docs "v"))))))))

(t/deftest test-await-fails-fast
  (let [e (UnsupportedOperationException. "oh no!")]
    (with-redefs [idx/->put-docs-indexer (fn [& _args]
                                           (throw e))
                  log/log* (let [log* log/log*]
                             (fn [logger level throwable message]
                               (when-not (identical? e throwable)
                                 (log* logger level throwable message))))]
      (t/is (anomalous? [:unsupported ::err/unsupported]
                        (xt/execute-tx tu/*node* [[:put-docs :xt_docs {:xt/id "foo", :count 42}]]))))))

(t/deftest bug-catch-closed-by-interrupt-exception-740
  (let [e (ClosedByInterruptException.)]
    (with-redefs [idx/->put-docs-indexer (fn [& _args]
                                           (throw e))
                  log/log* (let [log* log/log*]
                             (fn [logger level throwable message]
                               (when-not (identical? e throwable)
                                 (log* logger level throwable message))))]
      (t/is (thrown-with-msg? Exception #"Interrupted"
                              (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo(_id) VALUES (1)"]]))))))

(t/deftest test-indexes-sql-insert
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/can-index-sql-insert")]
      (util/delete-dir node-dir)

      (with-open [node (tu/->local-node {:node-dir node-dir
                                         :instant-src (tu/->mock-clock)})]
        (let [block-cat (.getBlockCatalog (db/primary-db node))]
          (t/is (nil? (.getCurrentBlockIndex block-cat)))

          (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01"))
                   (xt/execute-tx node [[:sql "INSERT INTO table (_id, foo, bar, baz) VALUES (?, ?, ?, ?)"
                                         [0, 2, "hello", 12]
                                         [1, 1, "world", 3.3]]])))

          (t/is (= {"xtdb" [(serde/->TxKey 0 (time/->instant #inst "2020-01-01"))]}
                   (xtp/latest-completed-txs node)))

          (tu/flush-block! node)

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-index-sql-insert")))
                         (.resolve node-dir "objects"))

          (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-index-sql-insert")))
                          (.resolve node-dir "objects")))))))

(t/deftest ingestion-stopped-query-as-tx-op-3265
  (t/is (anomalous? [:incorrect :xtdb/queries-in-read-write-tx
                     "Queries are unsupported in a DML transaction"
                     {:query "SELECT _id, foo FROM docs"}]
                    (xt/execute-tx tu/*node* [[:sql "SELECT _id, foo FROM docs"]]))))

(t/deftest above-max-long-halts-ingestion-3495
  (t/is (anomalous? [:incorrect :xtdb/sql-error #"Cannot parse integer: 9223372036854775808"]
                    (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id, foo) VALUES (9223372036854775808, 'bar')"]]))))

(t/deftest hyphen-in-struct-key-halts-ingestion-3388
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id, value) VALUES (1, {\"hyphen-bug\": 1}) "]])
  (t/is (= [{:xt/id 1, :value {:hyphen-bug 1}}]
           (xt/q tu/*node* "SELECT * FROM docs"))))

(t/deftest different-tzs-halt-ingestion-3483
  (xt/execute-tx tu/*node* [[:sql "
INSERT INTO docs (_id, _valid_from, _valid_to)
  VALUES (0, TIMESTAMP '2023-03-26T00:50:00.000+00:00', TIMESTAMP '2023-03-26T00:55:00.000+00:00'),
         (0, TIMESTAMP '2023-03-26T02:00:00.000+01:00', TIMESTAMP '2023-03-26T02:05:00.000+01:00')"]])

  (t/is (= [#:xt{:id 0,
                 :valid-from #xt/zoned-date-time "2023-03-26T00:50Z[UTC]",
                 :valid-to #xt/zoned-date-time "2023-03-26T00:55Z[UTC]"}
            #:xt{:id 0,
                 :valid-from #xt/zoned-date-time "2023-03-26T01:00Z[UTC]",
                 :valid-to #xt/zoned-date-time "2023-03-26T01:05Z[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME ORDER BY _valid_from"))))

(t/deftest test-snapshot-schema-is-updated-within-a-tx
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO t1(_id, foo) VALUES(1, 100)"]
                            [:sql "INSERT INTO t1(_id, foo, bar) (SELECT 2, 200, 2000)"]
                            [:sql "INSERT INTO t1(_id, foo, bar) (SELECT 3, x.foo, x.bar FROM (SELECT * FROM t1 WHERE bar = 2000) AS x)"]])

  (t/is (= [{:xt/id 2, :bar 2000, :foo 200}
            {:xt/id 1, :foo 100}
            {:xt/id 3, :bar 2000, :foo 200}]
           (xt/q tu/*node* "SELECT * FROM t1"))))

(t/deftest test-crash-log
  (let [node-dir (util/->path "target/indexer-test/test-crash-log")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir
                                       :instant-src (tu/->mock-clock)})]
      (xt/execute-tx node [[:put-docs :foo {:xt/id 2}]])

      (binding [idx/*crash-log-clock* (InstantSource/fixed Instant/EPOCH)]
        (let [node-id "xtdb-foo-node"
              ^BufferAllocator al (util/component node :xtdb/allocator)
              idxer (idx/<-node node)
              db (db/primary-db node)
              bp (.getBufferPool db)
              live-idx (.getLiveIndex db)
              tx-ops [[:put-docs :public/foo {:xt/id 3, :version 0}]
                      [:put-docs :public/foo {:xt/id 4, :version 0}]]]

          (with-open [db-idxer (.openForDatabase idxer (db/primary-db node))
                      live-idx-tx (.startTx live-idx (serde/->TxKey 1 Instant/EPOCH))
                      live-table-tx (.liveTable live-idx-tx #xt/table foo)
                      query-rel (Relation/openFromRows al [{:foo "bar", :baz 32}, {:foo "baz", :baz 64}])
                      tx-ops-rel (Relation/openFromArrowStream al (xt-log/serialize-tx-ops al
                                                                                           (mapv tx-ops/parse-tx-op tx-ops)
                                                                                           {:default-db "xtdb"
                                                                                            :system-time #xt/zdt "1970-01-01T00:00Z[UTC]"
                                                                                            :default-tz (ZoneId/of "Europe/London")}))]
            (let [tx-ops-rdr (.getListElements (.vectorFor tx-ops-rel "tx-ops"))] 
              (.logPut live-table-tx (ByteBuffer/allocate 16) 0 0
                       (fn []
                         (.writeObject (.getDocWriter live-table-tx) {:xt/id 3, :version 0})))
              (idx/crash-log! (-> db-idxer (assoc :node-id node-id)) "test crash log"
                              {:table #xt/table foo, :foo "bar"}
                              {:live-idx live-idx, :live-table-tx live-table-tx,
                               :query-rel query-rel, :tx-ops-rdr tx-ops-rdr})))

          (t/is (= {:table #xt/table foo, :foo "bar", :ex "test crash log"}
                   (let [path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/crash.edn" node-id))]
                     (-> (.getByteArray bp path)
                         String.
                         read-string))))

          (t/is (= {:tree [:leaf [0]], :page-limit 1024, :log-limit 64}
                   (let [path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/live-trie.binpb" node-id))]
                     (trie/<-MemoryHashTrie (MemoryHashTrie/fromProto (.getByteArray bp path) (NullVector. "_iid" 0))))))

          (t/is (= {:tree [:leaf [1 0]], :page-limit 1024, :log-limit 64}
                   (let [path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/live-trie-tx.binpb" node-id))]
                     (trie/<-MemoryHashTrie (MemoryHashTrie/fromProto (.getByteArray bp path) (NullVector. "_iid" 0))))))

          (let [live-table-tx-path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/live-table-tx.arrow" node-id))
                footer (.getFooter bp live-table-tx-path)]
            (with-open [rb (.getRecordBatch bp live-table-tx-path 0)
                        rel (Relation/fromRecordBatch al (.getSchema footer) rb)]
              (t/is (= [{:xt/system-from (time/->zdt #inst "2020"),
                         :xt/valid-from (time/->zdt #inst "2020"),
                         :xt/valid-to (time/->zdt time/end-of-time)
                         :op #xt/tagged [:put {:xt/id 2}]}
                        {:xt/system-from #xt/zdt "1970-01-01T00:00Z[UTC]",
                         :xt/valid-from #xt/zdt "1970-01-01T00:00Z[UTC]",
                         :xt/valid-to #xt/zdt "1970-01-01T00:00Z[UTC]",
                         :op #xt/tagged [:put {:xt/id 3, :version 0}]}]
                       (->> (.getAsMaps rel)
                            (mapv #(dissoc % :xt/iid)))))))

          (let [query-rel-path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/query-rel.arrow" node-id))
                footer (.getFooter bp query-rel-path)]
            (with-open [rb (.getRecordBatch bp query-rel-path 0)
                        rel (Relation/fromRecordBatch al (.getSchema footer) rb)]
              (t/is (= [{:foo "bar", :baz 32}, {:foo "baz", :baz 64}]
                       (->> (.getAsMaps rel)
                            (mapv #(dissoc % :xt/iid)))))))

          (let [tx-ops-path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/tx-ops.arrow" node-id))
                footer (.getFooter bp tx-ops-path)]
            (with-open [rb (.getRecordBatch bp tx-ops-path 0)
                        rel (Relation/fromRecordBatch al (.getSchema footer) rb)]
              (t/is (= [[{"_id" 3, "version" 0}] [{"_id" 4, "version" 0}]]
                       (-> (.vectorFor rel "$data$")
                           (.vectorFor "put-docs")
                           (.vectorFor "documents")
                           (.vectorFor "public/foo")
                           (.getAsList)))))))))))

(t/deftest list-concat-doesnt-halt-ingestion-3326
  ;; will likely have to remove this once we actually implement list concat
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :list [1 2]}]])

  (t/is (anomalous? [:incorrect nil #"^No matching clause: \[:list :i64\]$"]
                    (xt/execute-tx tu/*node* ["UPDATE docs SET list = list || [3]"])))

  (t/is (= [{:xt/id 1, :list [1 2]}]
           (xt/q tu/*node* "SELECT * FROM docs")))

  (t/is (= [{:xt/id 1,
             :committed false,
             :error #xt/error [:incorrect ::err/illegal-arg "No matching clause: [:list :i64]"
                               {:sql "UPDATE docs SET list = list || [3]", :tx-op-idx 0, :arg-idx 0
                                :tx-key #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-02T00:00:00Z"}}],
             :system-time #xt/zoned-date-time "2020-01-02T00:00Z[UTC]"}]
           (xt/q tu/*node* "SELECT * FROM xt.txs WHERE NOT committed"))))

(t/deftest test-copes-with-missing-put
  (let [node-dir (util/->path "target/indexer-test/copes-with-missing-put")]
    (util/delete-dir node-dir)
    (util/with-open [node (tu/->local-node {:node-dir node-dir
                                            :instant-src (tu/->mock-clock)
                                            :compactor-threads 0})]
      (xt/execute-tx node [[:put-docs :docs
                            {:xt/id "foo", :a 1}
                            {:xt/id "bar", :b 2}
                            {:xt/id "baz", :c 3}]])
      (tu/flush-block! node)
      (xt/execute-tx node [[:delete-docs :docs "foo"]])
      (tu/flush-block! node)
      (xt/execute-tx node [[:erase-docs :docs "bar"]])
      (tu/flush-block! node)
      (xt/execute-tx node [[:delete-docs :docs "baz"]
                           [:erase-docs :docs "baz"]])
      (tu/flush-block! node)

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/copes-with-missing-put")))
                     (.resolve node-dir "objects"))

      (t/is (= [{:xt/id "foo", :a 1,
                 :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
                 :xt/valid-to #xt/zdt "2020-01-02Z[UTC]",
                 :xt/system-from #xt/zdt "2020-01-01Z[UTC]"}
                {:xt/id "foo", :a 1,
                 :xt/valid-from #xt/zdt "2020-01-02Z[UTC]",
                 :xt/system-from #xt/zdt "2020-01-01Z[UTC]",
                 :xt/system-to #xt/zdt "2020-01-02Z[UTC]"}]
               (xt/q node "SELECT *, _valid_from, _valid_to, _system_from, _system_to
                           FROM docs FOR ALL VALID_TIME FOR ALL SYSTEM_TIME
                           ORDER BY _system_from, _valid_from"))))))
