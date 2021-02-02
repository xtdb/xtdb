(ns core2.ingest-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.core :as c2]
            [core2.ingest :as ingest]
            [core2.json :as c2-json]
            [core2.object-store :as os]
            [core2.log :as log]
            [core2.util :as util])
  (:import java.io.File
           java.util.Date
           [java.time Clock ZoneId]
           org.apache.arrow.memory.RootAllocator
           core2.log.LogRecord))

(defn- ->mock-clock ^java.time.Clock [^Iterable dates]
  (let [times-iterator (.iterator dates)]
    (proxy [Clock] []
      (getZone []
        (ZoneId/of "UTC"))
      (instant []
        (if (.hasNext times-iterator)
          (.toInstant ^Date (.next times-iterator))
          (throw (IllegalStateException. "out of time")))))))

(t/deftest can-build-chunk-as-arrow-ipc-file-format
  (let [object-dir (io/file "target/can-build-chunk-as-arrow-ipc-file-format/object-store")
        log-dir (io/file "target/can-build-chunk-as-arrow-ipc-file-format/log")
        mock-clock (->mock-clock [#inst "2020-01-01" #inst "2020-01-02"])]
    (util/delete-dir object-dir)
    (util/delete-dir log-dir)

    (with-open [a (RootAllocator. Long/MAX_VALUE)
                log-reader (log/->local-directory-log-reader log-dir)
                log-writer (log/->local-directory-log-writer log-dir {:clock mock-clock})
                os (os/->local-directory-object-store (.toPath object-dir))
                i (ingest/->ingester a os)]

      (doseq [tx-ops [[{:op :put
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
                              :mem-used 2.79257668E8}}]]]
        @(.appendRecord log-writer (c2/serialize-tx-ops tx-ops a)))

      (doseq [^LogRecord record (.readRecords log-reader nil Integer/MAX_VALUE)]
        (.indexTx i (ingest/->TransactionInstant (.offset record) (.time record)) (.record record)))

      (t/is (empty? @(.listObjects os)))
      (.finishChunk i)
      (let [objects-list @(.listObjects os)]
        (t/is (= 21 (count objects-list)))
        (t/is (= "metadata-00000000.arrow" (last objects-list)))))

    (c2-json/write-arrow-json-files object-dir)
    (t/is (= 42 (alength (.listFiles object-dir))))

    (doseq [^File f (.listFiles object-dir)
            :when (.endsWith (.getName f) ".json")]
      (t/is (= (json/parse-string (slurp (io/resource (str "can-build-chunk-as-arrow-ipc-file-format/" (.getName f)))))
               (json/parse-string (slurp f)))
            (.getName f)))))
