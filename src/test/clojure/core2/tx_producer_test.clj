(ns core2.tx-producer-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.test-json :as tj]
            [core2.test-util :as tu]
            [core2.tx-producer :as txp]
            [core2.util :as util]))

(t/use-fixtures :each tu/with-allocator)

(defn- test-serialize-tx-ops
  ([file tx-ops] (test-serialize-tx-ops file tx-ops {}))
  ([file tx-ops opts]
   (let [file (io/as-file file)
         actual (tj/arrow-streaming->json (txp/serialize-tx-ops tu/*allocator* tx-ops opts))]

     #_ ; uncomment this to reset the expected file (but don't commit it)
     (spit file actual)

     (t/is (= (json/parse-string (slurp file))
              (json/parse-string actual))))))

(t/deftest can-write-tx-to-arrow-ipc-streaming-format
  (test-serialize-tx-ops (io/resource "core2/tx-producer-test/can-write-tx.json")
                         [[:put 'device-info
                           {:id "device-info-demo000000",
                            :api-version "23",
                            :manufacturer "iobeam",
                            :model "pinto",
                            :os-name "6.0.1"}
                           {:app-time-start #inst "2022", :app-time-end #inst "2025"}]

                          [:put 'device-readings
                           {:id "reading-demo000000",
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
                            :mem-used 5.89988922E8}]

                          [:delete 'device-info "device-info-demo000000"
                           {:app-time-start #inst "2024", :app-time-end #inst "2025"}]

                          [:delete 'device-readings "reading-demo000000"]]))

(t/deftest can-write-tx-fn-calls
  (test-serialize-tx-ops (io/resource "core2/tx-producer-test/can-write-tx-fn-calls.json")
                         [[:call :foo 12 nil :bar]
                          [:call :foo2 "hello" "world"]]))

(t/deftest can-write-sql-to-arrow-ipc-streaming-format
  (test-serialize-tx-ops (io/resource "core2/tx-producer-test/can-write-sql.json")
                         [[:sql "INSERT INTO foo (id) VALUES (0)"]

                          [:sql "INSERT INTO foo (id, foo, bar) VALUES (?, ?, ?)"
                           [[1 nil 3.3]
                            [2 "hello" 12]]]

                          [:sql "UPDATE foo FOR PORTION OF APP_TIME FROM DATE '2021-01-01' TO DATE '2024-01-01' SET bar = 'world' WHERE foo.id = ?"
                           [[1]]]

                          [:sql "DELETE FROM foo FOR PORTION OF APP_TIME FROM DATE '2023-01-01' TO DATE '2025-01-01' WHERE foo.id = ?"
                           [[1]]]]))

(t/deftest can-write-opts
  (test-serialize-tx-ops (io/resource "core2/tx-producer-test/can-write-opts.json")
                         [[:sql "INSERT INTO foo (id) VALUES (0)"]]

                         {:sys-time (util/->instant #inst "2021")
                          :app-time-as-of-now? true
                          :default-tz #time/zone "Europe/London"}))
