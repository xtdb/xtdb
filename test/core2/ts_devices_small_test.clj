(ns core2.ts-devices-small-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.data.csv :as csv]
            [core2.ts-devices :as ts]
            [core2.core :as c2]
            [core2.util :as util]
            [core2.metadata :as meta]
            [core2.test-util :as tu])
  (:import core2.metadata.IMetadataManager
           [java.time Duration]))

(t/deftest ^:integration can-ingest-ts-devices-small
  (if-not (io/resource "devices_small_device_info.csv")
    (t/is true)
    (let [node-dir (util/->path "target/can-ingest-ts-devices-small")]
      (util/delete-dir node-dir)

      (with-open [node (tu/->local-node {:node-dir node-dir})
                  info-reader (io/reader (io/resource "devices_small_device_info.csv"))
                  readings-reader (io/reader (io/resource "devices_small_readings.csv"))]
        (let [^IMetadataManager mm (:core2/metadata-manager @(:!system node))
              device-infos (mapv ts/device-info-csv->doc (csv/read-csv info-reader))
              readings (mapv ts/readings-csv->doc (csv/read-csv readings-reader))
              [initial-readings rest-readings] (split-at (count device-infos) readings)
              tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                       {:op :put
                        :doc doc})]

          (t/is (= 1001000 (count tx-ops)))

          (t/is (nil? (c2/latest-completed-tx node)))

          (let [last-tx-instant @(reduce
                                  (fn [_acc tx-ops]
                                    (c2/submit-tx node tx-ops))
                                  nil
                                  (partition-all 100 tx-ops))]

            (t/is (= last-tx-instant (c2/await-tx node last-tx-instant (Duration/ofMinutes 15))))
            (t/is (= last-tx-instant (c2/latest-completed-tx node)))
            (tu/finish-chunk node)

            (t/is (= [last-tx-instant (dec (count tx-ops))]
                     @(meta/with-latest-metadata mm
                        (juxt meta/latest-tx meta/latest-row-id))))))))))
