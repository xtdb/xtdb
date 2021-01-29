(ns core2.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.core :as c2]
            [core2.json :as c2-json]
            [cheshire.core :as json])
  (:import java.io.File
           org.apache.arrow.memory.RootAllocator))

(t/deftest can-write-tx-to-arrow-ipc-streaming-format
  (with-open [a (RootAllocator. Long/MAX_VALUE)]
    (t/is (= (json/parse-string (slurp (io/resource "tx-ipc.json")))
             (-> (c2/submit-tx [{:op :put
                                 :doc {:_id "device-info-demo000000",
                                       :api-version "23",
                                       :manufacturer "iobeam",
                                       :model "pinto",
                                       :os-name "6.0.1"}}
                                {:op :put
                                 :doc {:_id "reading-demo000000",
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
                               a)
                 (c2-json/arrow-streaming->json)
                 (json/parse-string))))))

(t/deftest can-build-chunk-as-arrow-ipc-file-format
  (let [ingest-dir (doto (io/file "target/test-ingest")
                     (.mkdir))]
    (doseq [^File f (.listFiles ingest-dir)]
      (.delete f))
    (t/is (zero? (alength (.listFiles ingest-dir))))
    (with-open [a (RootAllocator. Long/MAX_VALUE)
                i (c2/->ingester a ingest-dir)]

      (doseq [[tx ops] [[{:tx-time #inst "2020-01-01"
                          :tx-id 1}
                         [{:op :put
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
                                 :mem-used 5.89988922E8}}]]
                        [{:tx-time #inst "2020-01-02"
                          :tx-id 2}
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
                                 :mem-used 2.79257668E8}}]]]]
        (c2/index-tx i tx (c2/submit-tx ops a))))

    (c2-json/write-arrow-json-files ingest-dir)
    (t/is (= 42 (alength (.listFiles ingest-dir))))

    (doseq [^File f (.listFiles ingest-dir)
            :when (.endsWith (.getName f) ".json")]
      (t/is (= (json/parse-string (slurp (io/resource (str "ingest/" (.getName f)))))
               (json/parse-string (slurp f)))
            (.getName f)))))
