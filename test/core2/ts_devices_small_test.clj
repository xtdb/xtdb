(ns core2.ts-devices-small-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.data.csv :as csv]
            [core2.ts-devices :as ts]
            [core2.core :as c2]
            [core2.util :as util])
  (:import [core2.core IngestLoop Node]
           [core2.indexer Indexer]
           core2.metadata.IMetadataManager
           [java.time Duration]))

(t/deftest can-ingest-ts-devices-small
  (if-not (io/resource "devices_small_device_info.csv")
    (t/is true)
    (let [node-dir (util/->path "target/can-ingest-ts-devices-small")]
      (util/delete-dir node-dir)

      (with-open [node (c2/->local-node node-dir)
                  tx-producer (c2/->local-tx-producer node-dir {})
                  info-reader (io/reader (io/resource "devices_small_device_info.csv"))
                  readings-reader (io/reader (io/resource "devices_small_readings.csv"))]
        (let [^Indexer i (.indexer node)
              ^IngestLoop il (.ingest-loop node)
              ^IMetadataManager mm (.metadata-manager node)
              device-infos (mapv ts/device-info-csv->doc (csv/read-csv info-reader))
              readings (mapv ts/readings-csv->doc (csv/read-csv readings-reader))
              [initial-readings rest-readings] (split-at (count device-infos) readings)
              tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                       {:op :put
                        :doc doc})]

          (t/is (= 1001000 (count tx-ops)))

          (t/is (nil? (.latestCompletedTx il)))

          (let [last-tx-instant @(reduce
                                  (fn [acc tx-ops]
                                    (.submitTx tx-producer tx-ops))
                                  nil
                                  (partition-all 100 tx-ops))]

            (t/is (= last-tx-instant (.awaitTx il last-tx-instant (Duration/ofMinutes 15))))
            (t/is (= last-tx-instant (.latestCompletedTx il)))
            (.finishChunk i)

            (t/is (= last-tx-instant (.latestStoredTx mm)))
            (t/is (= (dec (count tx-ops)) (.latestStoredRowId mm)))))))))
