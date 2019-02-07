(ns crux.ts-devices-test
  (:require [clojure.test :as t]
            [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.tx :as tx]
            [crux.lru :as lru]
            [crux.query :as q]
            [crux.kafka :as k]
            [crux.fixtures :as f])
  (:import java.math.RoundingMode
           java.util.Date
           java.time.temporal.ChronoUnit))

;; https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-devices
;; Requires https://timescaledata.blob.core.windows.net/datasets/devices_small.tar.gz

(def ^:const devices-device-info-csv-resource "ts/data/devices_small_device_info.csv")
(def ^:const devices-readings-csv-resource "ts/data/devices_small_readings.csv")

(def run-ts-devices-tests? (boolean (and (io/resource devices-device-info-csv-resource)
                                         (io/resource devices-readings-csv-resource)
                                         (System/getenv "CRUX_TS_DEVICES"))))

(defn with-kv-backend-from-env [f]
  (binding [f/*kv-backend* (or (System/getenv "CRUX_TS_KV_BACKEND") f/*kv-backend*)]
    (when run-ts-devices-tests?
      (println "Using KV backend:" f/*kv-backend*))
    (f)))

(def ^:const readings-chunk-size 1000)

(defn submit-ts-devices-data
  ([tx-log]
   (submit-ts-devices-data tx-log (io/resource devices-device-info-csv-resource) (io/resource devices-readings-csv-resource)))
  ([tx-log info-resource readings-resource]
   (with-open [info-in (io/reader info-resource)
               readings-in (io/reader readings-resource)]
     (let [info-tx-ops (vec (for [device-info (line-seq info-in)
                                  :let [[device-id api-version manufacturer model os-name] (str/split device-info #",")
                                        id (keyword "device-info" device-id)]]
                              [:crux.tx/put
                               id
                               {:crux.db/id id
                                :device-info/api-version api-version
                                :device-info/manufacturer manufacturer
                                :device-info/model model
                                :device-info/os-name os-name}]))]
       (db/submit-tx tx-log info-tx-ops)
       (->> (line-seq readings-in)
            (partition readings-chunk-size)
            (reduce
             (fn [n chunk]
               (db/submit-tx
                tx-log
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
                        reading-id
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
    (let [tx-topic "test-devices-ts-tx"
          doc-topic "test-devices-ts-doc"
          tx-log (k/->KafkaTxLog f/*producer* tx-topic doc-topic {})
          object-store (lru/new-cached-object-store f/*kv*)
          indexer (tx/->KvIndexer f/*kv* tx-log object-store)]

      (k/create-topic f/*admin-client* tx-topic 1 1 k/tx-topic-config)
      (k/create-topic f/*admin-client* doc-topic 1 1 k/doc-topic-config)
      (k/subscribe-from-stored-offsets indexer f/*consumer* [tx-topic doc-topic])
      (let [submit-future (future
                            (submit-ts-devices-data tx-log))
            consume-args {:indexer indexer
                          :consumer f/*consumer*
                          :pending-txs-state (atom [])
                          :tx-topic tx-topic
                          :doc-topic doc-topic}]
        (k/consume-and-index-entities consume-args)
        (while (not= {:txs 0 :docs 0}
                     (k/consume-and-index-entities
                      (assoc consume-args :timeout 100))))
        (t/is (= 1001000 @submit-future))
        (tx/await-no-consumer-lag indexer {:crux.tx-log/await-tx-timeout 60000}))
      (f))
    (f)))

(t/use-fixtures :once f/with-embedded-kafka-cluster f/with-kafka-client with-kv-backend-from-env f/with-kv-store with-ts-devices-data)

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
    (t/is true)
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

(t/deftest test-busiest-devices-1-min-avg-whoste-battery-evel-is-below-33-percent-and-is-not-charging
  (if run-ts-devices-tests?
    (t/is true)
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
    (t/is true)
    (t/is true)))
