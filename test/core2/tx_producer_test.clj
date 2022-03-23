(ns core2.tx-producer-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.json :as c2-json]
            [core2.tx-producer :as txp]
            [core2.util :as util])
  (:import org.apache.arrow.memory.RootAllocator))

(def ^:private expected-file
  (io/as-file (io/resource "can-write-tx-to-arrow-ipc-streaming-format/expected.json")))

(t/deftest can-write-tx-to-arrow-ipc-streaming-format
  (with-open [a (RootAllocator.)]
    (t/is (= (json/parse-string (slurp expected-file))
             (-> (txp/serialize-tx-ops
                  [[:put {:_id "device-info-demo000000",
                          :api-version "23",
                          :manufacturer "iobeam",
                          :model "pinto",
                          :os-name "6.0.1"}]
                   [:put {:_id "reading-demo000000",
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
                  {:tx-time (util/->instant #inst "2021")}
                  a)
                 (c2-json/arrow-streaming->json)
                 #_(doto (->> (spit expected-file)))
                 (json/parse-string))))))
