(ns crux.ts-devices-test
  (:require [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures.api :refer [*api*]]
            [crux.fixtures.kafka :as fk]
            [crux.fixtures.kv-only :as fkv :refer [*kv*]]
            [crux.io :as cio])
  (:import java.time.temporal.ChronoUnit
           java.util.Date))

;; https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-devices
;; Requires https://timescaledata.blob.core.windows.net/datasets/devices_small.tar.gz

(def ^:const devices-device-info-csv-resource "ts/data/devices_small_device_info.csv")
(def ^:const devices-readings-csv-resource "ts/data/devices_small_readings.csv")

(def run-ts-devices-tests? (boolean (and (io/resource devices-device-info-csv-resource)
                                         (io/resource devices-readings-csv-resource)
                                         (Boolean/parseBoolean (System/getenv "CRUX_TS_DEVICES")))))

(def ^:const readings-chunk-size 1000)

;; Submits data from devices database into Crux node.
(defn submit-ts-devices-data
  ([node]
   (submit-ts-devices-data
     node
     (io/resource devices-device-info-csv-resource)
     (io/resource devices-readings-csv-resource)))
  ([node info-resource readings-resource]
   (with-open [info-in (io/reader info-resource)
               readings-in (io/reader readings-resource)]
     (let [info-tx-ops
           (vec (for [device-info (line-seq info-in)
                      :let [[device-id api-version manufacturer model os-name] (str/split device-info #",")
                            id (keyword "device-info" device-id)]]
                  [:crux.tx/put
                   {:crux.db/id id
                    :device-info/api-version api-version
                    :device-info/manufacturer manufacturer
                    :device-info/model model
                    :device-info/os-name os-name}]))]
       (api/submit-tx node info-tx-ops)
       (->> (line-seq readings-in)
            (partition readings-chunk-size)
            (reduce
             (fn [n chunk]
               (api/submit-tx
                node
                (vec (for [reading chunk
                           :let [[time device-id battery-level battery-status
                                  battery-temperature bssid
                                  cpu-avg-1min cpu-avg-5min cpu-avg-15min
                                  mem-free mem-used rssi ssid] (str/split reading #",")
                                 time (inst/read-instant-date
                                       (-> time
                                           (str/replace " " "T")
                                           (str/replace #"-(\d\d)$" ".000-$1:00")))
                                 reading-id (keyword "reading" device-id)
                                 device-id (keyword "device-info" device-id)]]
                       [:crux.tx/put
                        {:crux.db/id reading-id
                         :reading/time time
                         :reading/device-id device-id
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
                         :reading/ssid ssid}
                        time])))
               (+ n (count chunk)))
             (count info-tx-ops)))))))

(defn with-ts-devices-data [f]
  (if run-ts-devices-tests?
    (let [submit-future (future (submit-ts-devices-data *api*))]
      (api/sync *api* (java.time.Duration/ofMinutes 20))
      (t/is (= 1001000 @submit-future))
      (f))
    (f)))

(t/use-fixtures :once
                fk/with-embedded-kafka-cluster
                fk/with-kafka-client
                fk/with-cluster-node-opts
                ; perhaps should use with-node as well. if this config fails try uncommenting the line below
                ; f-api/with-node
                with-ts-devices-data)

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

(t/deftest test-10-most-recent-battery-readeings-for-charging-devices
  (if run-ts-devices-tests?
    (t/is (= [[#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000999 88.7]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000998 93.1]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000997 90.7]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000996 92.8]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000995 91.9]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000994 92.0]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000993 92.8]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000992 87.6]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000991 93.1]
              [#inst "2016-11-15T20:19:30.000-00:00" :device-info/demo000990 89.9]])
          (api/q (api/db *api*)
              '{:find [time device-id battery-temperature]
                :where [[r :reading/time time]
                        [r :reading/device-id device-id]
                        [r :reading/battery-temperature battery-temperature]]
                :order-by [[time :desc] [device-id :desc]]
                :limit 10}))
    (t/is true)))

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
(t/deftest test-busiest-devices-1-min-avg-whoste-battery-evel-is-below-33-percent-and-is-not-charging
  (if run-ts-devices-tests?
    (t/is (= [[#inst "2016-11-15T20:19:30.000-00:00"
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
               "focus"]])
          (api/q (api/db *api*)
              '{:find [time device-id cpu-avg-1min battery-level battery-status model]
                :where [[r :reading/time time]
                        [r :reading/device-id device-id]
                        [r :reading/cpu-avg-1min cpu-avg-1min]
                        [r :reading/battery-level battery-level]
                        [(< battery-level 33.0)]
                        [r :reading/battery-status :discharging]
                        [r :reading/battery-status battery-status]
                        [device-id :device-info/model model]]
                :order-by [[cpu-avg-1min :desc] [time :desc]]
                :limit 5}))
    (t/is true)))

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

(t/deftest test-min-max-battery-level-per-hour-for-pinto-or-focus-devices
  (if run-ts-devices-tests?
    (t/is (= [[#inst "2016-11-15T12:00:00.000-00:00" 20.0 99.0]
              [#inst "2016-11-15T13:00:00.000-00:00" 13.0 100.0]
              [#inst "2016-11-15T14:00:00.000-00:00" 9.0 100.0]
              [#inst "2016-11-15T15:00:00.000-00:00" 6.0 100.0]
              [#inst "2016-11-15T16:00:00.000-00:00" 6.0 100.0]
              [#inst "2016-11-15T17:00:00.000-00:00" 6.0 100.0]
              [#inst "2016-11-15T18:00:00.000-00:00" 6.0 100.0]
              [#inst "2016-11-15T19:00:00.000-00:00" 6.0 100.0]
              [#inst "2016-11-15T20:00:00.000-00:00" 6.0 100.0]]
             (let [kv *kv*
                   reading-ids (->> (api/q (api/db *api*)
                                        '{:find [r]
                                          :where [[r :reading/device-id device-id]
                                                  (or [device-id :device-info/model "pinto"]
                                                      [device-id :device-info/model "focus"])]})
                                    (reduce into []))
                   db (api/db *api* #inst "1970")]
               (with-open [snapshot (api/new-snapshot db)]
                 (->> (for [r reading-ids]
                        (for [entity-tx (api/history-ascending db snapshot r)]
                          (update entity-tx :crux.db/valid-time #(Date/from (.truncatedTo (.toInstant ^Date %) ChronoUnit/HOURS)))))
                      (cio/merge-sort (fn [a b]
                                        (compare (:crux.db/valid-time a) (:crux.db/valid-time b))))
                      (partition-by :crux.db/valid-time)
                      (take 12)
                      (mapv (fn [group]
                              (let [battery-levels (sort (mapv (comp :reading/battery-level :crux.db/doc) group))]
                                [(:crux.db/valid-time (first group))
                                 (first battery-levels)
                                 (last battery-levels)]))))))))
    (t/is true)))
