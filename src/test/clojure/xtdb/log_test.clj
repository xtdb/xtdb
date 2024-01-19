(ns xtdb.log-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.log :as log]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]))

(t/use-fixtures :each tu/with-allocator)

(defn- test-serialize-tx-ops
  ([file tx-ops] (test-serialize-tx-ops file tx-ops {}))
  ([file tx-ops opts]
   (binding [*print-namespace-maps* false]
     (let [file (io/as-file file)
           actual (tj/arrow-streaming->json (log/serialize-tx-ops tu/*allocator* tx-ops opts))]

       ;; uncomment this to reset the expected file (but don't commit it)
       #_(spit file actual) ;; <<no-commit>>

       (t/is (= (tj/sort-arrow-json (json/read-value (slurp file)))
                (tj/sort-arrow-json (json/read-value actual))))))))

(def devices-docs
  [(xt/put :device-info
           {:xt/id "device-info-demo000000",
            :api-version "23",
            :manufacturer "iobeam",
            :model "pinto",
            :os-name "6.0.1"})
   (xt/put :device-readings
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
            :mem-used 5.89988922E8})
   (xt/put :device-info
           {:xt/id "device-info-demo000001",
            :api-version "23",
            :manufacturer "iobeam",
            :model "mustang",
            :os-name "6.0.1"})
   (xt/put :device-readings
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
            :mem-used 2.79257668E8})])

(t/deftest can-write-tx-to-arrow-ipc-streaming-format
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-tx.json") devices-docs))

(t/deftest can-write-put-fns
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-put-fns.json")
                         [(xt/put-fn :foo '(fn [id] [(xt/put :foo {:xt/id id})]))
                          (-> (xt/put-fn :bar '(fn [id] [(xt/put :bar {:xt/id id})]))
                              (xt/starting-from #inst "2020"))]))

(t/deftest can-write-tx-fn-calls
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-tx-fn-calls.json")
                         [(xt/call :foo 12 nil :bar)
                          (xt/call :foo2 "hello" "world")]))

(t/deftest can-write-docs-with-different-keys
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/docs-with-different-keys.json")
                         [(xt/put :foo {:xt/id :a, :a 1})
                          (xt/put :foo {:xt/id "b", :b 2})
                          (xt/put :bar {:xt/id 3, :c 3})]))

(t/deftest can-write-sql-to-arrow-ipc-streaming-format
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-sql.json")
                         [(-> [:sql "INSERT INTO foo (xt$id) VALUES (0)"]
                              (tx-ops/parse-tx-op))

                          (-> [:sql "INSERT INTO foo (xt$id, foo, bar) VALUES (?, ?, ?)"
                               [1 nil 3.3]
                               [2 "hello" 12]]
                              (tx-ops/parse-tx-op))

                          (-> [:sql "UPDATE foo FOR PORTION OF VALID_TIME FROM DATE '2021-01-01' TO DATE '2024-01-01' SET bar = 'world' WHERE foo.xt$id = ?"
                               [1]]
                              (tx-ops/parse-tx-op))

                          (-> [:sql "DELETE FROM foo FOR PORTION OF VALID_TIME FROM DATE '2023-01-01' TO DATE '2025-01-01' WHERE foo.xt$id = ?"
                               [1]]
                              (tx-ops/parse-tx-op))]))

(t/deftest can-write-xtql
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-xtdml.json")
                         (->> [[:update '{:table :users
                                          :bind [{:xt/id $uid, :version v}]
                                          :set {:v (inc v)}}
                                {:uid :jms}]

                               [:delete '{:from :users, :bind [{:xt/id $uid}]}
                                {:uid :jms}]]
                              (mapv tx-ops/parse-tx-op))))

(t/deftest can-write-opts
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-opts.json")
                         [(-> [:sql "INSERT INTO foo (id) VALUES (0)"]
                              (tx-ops/parse-tx-op))]

                         {:system-time (time/->instant #inst "2021")
                          :default-all-valid-time? false
                          :default-tz #time/zone "Europe/London"}))
