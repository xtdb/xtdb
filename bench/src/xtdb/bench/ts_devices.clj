(ns xtdb.bench.ts-devices
  (:require [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [crux.api :as xt]
            [xtdb.bench :as bench]
            [crux.io :as cio])
  (:import java.time.Duration
           java.time.temporal.ChronoUnit
           java.util.Date))

;; https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-devices
;; Requires https://timescaledata.blob.core.windows.net/datasets/devices_small.tar.gz

(def device-info-csv-resource (io/resource "devices_small_device_info.csv"))
(def readings-csv-resource (io/resource "devices_small_readings.csv"))

(def ^:const readings-chunk-size 1000)

(def info-docs
  (delay
    (when device-info-csv-resource
      (with-open [rdr (io/reader device-info-csv-resource)]
        (vec (for [device-info (line-seq rdr)
                   :let [[device-id api-version manufacturer model os-name] (str/split device-info #",")]]
               {:xt/id (keyword "device-info" device-id)
                :device-info/api-version api-version
                :device-info/manufacturer manufacturer
                :device-info/model model
                :device-info/os-name os-name}))))))

(def ^:dynamic *readings-limit* nil)

(defn with-readings-docs [f]
  (when readings-csv-resource
    (with-open [rdr (io/reader readings-csv-resource)]
      (f (cond->> (for [reading (line-seq rdr)
                        :let [[time device-id battery-level battery-status
                               battery-temperature bssid
                               cpu-avg-1min cpu-avg-5min cpu-avg-15min
                               mem-free mem-used rssi ssid] (str/split reading #",")]]
                    {:xt/id (keyword "reading" device-id)
                     :reading/time (inst/read-instant-date
                                    (-> time
                                        (str/replace " " "T")
                                        (str/replace #"-(\d\d)$" ".000-$1:00")))
                     :reading/device-id (keyword "device-info" device-id)
                     :reading/battery-level (Double/parseDouble battery-level)
                     :reading/battery-status (keyword battery-status)
                     :reading/battery-temperature (Double/parseDouble battery-temperature)
                     :reading/bssid bssid
                     :reading/cpu-avg-1min (Double/parseDouble cpu-avg-1min)
                     :reading/cpu-avg-5min (Double/parseDouble cpu-avg-5min)
                     :reading/cpu-avg-15min (Double/parseDouble cpu-avg-15min)
                     :reading/mem-free (Double/parseDouble mem-free)
                     :reading/mem-used (Double/parseDouble mem-used)
                     :reading/rssi (Double/parseDouble rssi)
                     :reading/ssid ssid})
           *readings-limit* (take *readings-limit*))))))

;; Submits data from devices database into Crux node.
(defn submit-ts-devices-data [node]
  (let [info-tx-ops (vec (for [info-doc @info-docs]
                           [:xt/put info-doc]))
        _ (xt/submit-tx node info-tx-ops)
        last-tx (with-readings-docs
                  (fn [readings-docs]
                    (->> readings-docs
                         (partition-all readings-chunk-size)
                         (reduce (fn [last-tx chunk]
                                   (xt/submit-tx node (vec (for [{:keys [reading/time] :as reading-doc} chunk]
                                                               [:xt/put reading-doc time]))))
                                 nil))))]
    (xt/await-tx node last-tx (Duration/ofMinutes 20))
    {:success? true}))

(defn test-battery-readings [node]
  ;; 10 most recent battery temperature readings for charging devices
  ;; SELECT time, device_id, battery_temperature
  ;; FROM readings
  ;; WHERE battery_status = 'charging'
  ;; ORDER BY time DESC LIMIT 10;

  ;; time                   | device_id  | battery_temperature
  ;; -----------------------+------------+---------------------
  ;; 2016-11-15 23:39:30-05 | demo004887 |                99.3
  ;; 2016-11-15 23:39:30-05 | demo004882 |               100.8
  ;; 2016-11-15 23:39:30-05 | demo004862 |                95.7
  ;; 2016-11-15 23:39:30-05 | demo004844 |                95.5
  ;; 2016-11-15 23:39:30-05 | demo004841 |                95.4
  ;; 2016-11-15 23:39:30-05 | demo004804 |               101.6
  ;; 2016-11-15 23:39:30-05 | demo004784 |               100.6
  ;; 2016-11-15 23:39:30-05 | demo004760 |                99.1
  ;; 2016-11-15 23:39:30-05 | demo004731 |                97.9
  ;; 2016-11-15 23:39:30-05 | demo004729 |                99.6
  ;; (10 rows)

  (bench/run-bench :recent-battery-readings
    (let [query '{:find [time device-id battery-temperature]
                  :where [[r :reading/time time]
                          [r :reading/device-id device-id]
                          [r :reading/battery-temperature battery-temperature]]
                  :order-by [[time :desc] [device-id :desc]]
                  :limit 10}
          success? (= (xt/q (xt/db node) query)
                      [[#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000999 88.7]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000998 93.1]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000997 90.7]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000996 92.8]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000995 91.9]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000994 92.0]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000993 92.8]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000992 87.6]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000991 93.1]
                       [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000990 89.9]])]
      {:success? success?})))

(defn test-busiest-devices [node]
  ;; Busiest devices (1 min avg) whose battery level is below 33% and is not charging

  ;; SELECT time, readings.device_id, cpu_avg_1min,
  ;; battery_level, battery_status, device_info.model
  ;; FROM readings
  ;; JOIN device_info ON readings.device_id = device_info.device_id
  ;; WHERE battery_level < 33 AND battery_status = 'discharging'
  ;; ORDER BY cpu_avg_1min DESC, time DESC LIMIT 5;

  ;; time                   | device_id  | cpu_avg_1min | battery_level | battery_status |  model
  ;; -----------------------+------------+--------------+---------------+----------------+---------
  ;; 2016-11-15 23:30:00-05 | demo003764 |        98.99 |            32 | discharging    | focus
  ;; 2016-11-15 22:54:30-05 | demo001935 |        98.99 |            30 | discharging    | pinto
  ;; 2016-11-15 19:10:30-05 | demo000695 |        98.99 |            23 | discharging    | focus
  ;; 2016-11-15 16:46:00-05 | demo002784 |        98.99 |            18 | discharging    | pinto
  ;; 2016-11-15 14:58:30-05 | demo004978 |        98.99 |            22 | discharging    | mustang
  ;; (5 rows)

  ;; TODO: This test doesn't only does current time slice, which isn't
  ;; valid for this example.

  (bench/run-bench :busiest-devices
    (let [query '{:find [time device-id cpu-avg-1min battery-level battery-status model]
                  :where [[r :reading/time time]
                          [r :reading/device-id device-id]
                          [r :reading/cpu-avg-1min cpu-avg-1min]
                          [r :reading/battery-level battery-level]
                          [(< battery-level 33.0)]
                          [r :reading/battery-status :discharging]
                          [r :reading/battery-status battery-status]
                          [device-id :device-info/model model]]
                  :order-by [[cpu-avg-1min :desc] [time :desc]]
                  :limit 5}

          success? (= (xt/q (xt/db node) query)
                      [[#inst "2016-11-15T20:19:30.000-00:00"
                        :device-info/demo000818
                        33.45
                        26.0
                        :discharging
                        "focus"]
                       [#inst "2016-11-15T20:19:30.000-00:00"
                        :device-info/demo000278
                        32.59
                        14.0
                        :discharging
                        "focus"]
                       [#inst "2016-11-15T20:19:30.000-00:00"
                        :device-info/demo000418
                        32.11
                        18.0
                        :discharging
                        "mustang"]
                       [#inst "2016-11-15T20:19:30.000-00:00"
                        :device-info/demo000942
                        31.72
                        26.0
                        :discharging
                        "pinto"]
                       [#inst "2016-11-15T20:19:30.000-00:00"
                        :device-info/demo000800
                        31.34
                        25.0
                        :discharging
                        "focus"]])]
      {:success? success?})))


(defn test-min-max-battery-level-per-hour [node]
  ;; min max battery level per hour for pinto or focus devices

  ;; SELECT date_trunc('hour', time) "hour",
  ;; min(battery_level) min_battery_level,
  ;; max(battery_level) max_battery_level
  ;; FROM readings r
  ;; WHERE r.device_id IN (
  ;;     SELECT DISTINCT device_id FROM device_info
  ;;     WHERE model = 'pinto' OR model = 'focus'
  ;; ) GROUP BY "hour" ORDER BY "hour" ASC LIMIT 12;

  ;; hour                   | min_battery_level | max_battery_level
  ;; -----------------------+-------------------+-------------------
  ;; 2016-11-15 07:00:00-05 |                17 |                99
  ;; 2016-11-15 08:00:00-05 |                11 |                98
  ;; 2016-11-15 09:00:00-05 |                 6 |                97
  ;; 2016-11-15 10:00:00-05 |                 6 |                97
  ;; 2016-11-15 11:00:00-05 |                 6 |                97
  ;; 2016-11-15 12:00:00-05 |                 6 |                97
  ;; 2016-11-15 13:00:00-05 |                 6 |                97
  ;; 2016-11-15 14:00:00-05 |                 6 |                98
  ;; 2016-11-15 15:00:00-05 |                 6 |               100
  ;; 2016-11-15 16:00:00-05 |                 6 |               100
  ;; 2016-11-15 17:00:00-05 |                 6 |               100
  ;; 2016-11-15 18:00:00-05 |                 6 |               100
  ;; (12 rows)

  (bench/run-bench :min-max-battery-level-per-hour
    (with-open [db (xt/open-db node)]
      (let [result (let [reading-ids (->> (xt/q db
                                                  '{:find [r]
                                                    :where [[r :reading/device-id device-id]
                                                            (or [device-id :device-info/model "pinto"]
                                                                [device-id :device-info/model "focus"])]})
                                          (reduce into []))
                         histories (for [r reading-ids]
                                     (xt/open-entity-history db r :asc {:with-docs? true
                                                                          :start {:xt/valid-time  #inst "1970"}}))]
                     (try
                       (->> (for [history histories]
                              (for [entity-tx (iterator-seq history)]
                                (update entity-tx :xt/valid-time #(Date/from (.truncatedTo (.toInstant ^Date %) ChronoUnit/HOURS)))))
                            (cio/merge-sort (fn [a b]
                                              (compare (:xt/valid-time a) (:xt/valid-time b))))
                            (partition-by :xt/valid-time)
                            (take 12)
                            (mapv (fn [group]
                                    (let [battery-levels (sort (mapv (comp :reading/battery-level :xt/doc) group))]
                                      [(:xt/valid-time (first group))
                                       (first battery-levels)
                                       (last battery-levels)]))))
                       (finally
                         (run! cio/try-close histories))))

            success? (= [[#inst "2016-11-15T12:00:00.000-00:00" 20.0 99.0]
                         [#inst "2016-11-15T13:00:00.000-00:00" 13.0 100.0]
                         [#inst "2016-11-15T14:00:00.000-00:00" 9.0 100.0]
                         [#inst "2016-11-15T15:00:00.000-00:00" 6.0 100.0]
                         [#inst "2016-11-15T16:00:00.000-00:00" 6.0 100.0]
                         [#inst "2016-11-15T17:00:00.000-00:00" 6.0 100.0]
                         [#inst "2016-11-15T18:00:00.000-00:00" 6.0 100.0]
                         [#inst "2016-11-15T19:00:00.000-00:00" 6.0 100.0]
                         [#inst "2016-11-15T20:00:00.000-00:00" 6.0 100.0]]
                        result)]

        {:success? success?}))))

(defn run-devices-bench [node]
  (bench/with-bench-ns :ts-devices
    (bench/with-crux-dimensions
      (bench/run-bench :ingest
        (bench/with-additional-index-metrics node
          (submit-ts-devices-data node)))

      (bench/compact-node node)
      (test-battery-readings node)
      (test-busiest-devices node)
      (test-min-max-battery-level-per-hour node))))

(comment
  (binding [*readings-limit* 1000]
    (bench/with-nodes [node (select-keys bench/nodes ["standalone-rocksdb"])]
      (bench/with-bench-ns :ts-devices
        (bench/with-crux-dimensions
          (submit-ts-devices-data node)
          (bench/compact-node node))))))
