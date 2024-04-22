(ns xtdb.ts-devices
  (:require [clojure.data.csv :as csv]
            [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [xtdb.api :as xt])
  (:import java.util.zip.GZIPInputStream))

(defn device-info-csv->doc [[device-id api-version manufacturer model os-name]]
  (with-meta {:xt/id device-id
              :device-id device-id
              :api-version api-version
              :manufacturer manufacturer
              :model model
              :os-name os-name}
    {:table :device-info}))

(defn readings-csv->doc [[time device-id battery-level battery-status
                          battery-temperature bssid
                          cpu-avg-1min cpu-avg-5min cpu-avg-15min
                          mem-free mem-used rssi ssid]]
  (with-meta {:xt/id device-id
              :time (inst/read-instant-date
                     (-> time
                         (str/replace " " "T")
                         (str/replace #"-(\d\d)$" ".000-$1:00")))
              :device-id device-id
              :battery-level (Double/parseDouble battery-level)
              :battery-status battery-status
              :battery-temperature (Double/parseDouble battery-temperature)
              :bssid bssid
              :cpu-avg-1min (Double/parseDouble cpu-avg-1min)
              :cpu-avg-5min (Double/parseDouble cpu-avg-5min)
              :cpu-avg-15min (Double/parseDouble cpu-avg-15min)
              :mem-free (Double/parseDouble mem-free)
              :mem-used (Double/parseDouble mem-used)
              :rssi (Double/parseDouble rssi)
              :ssid ssid}
    {:table :device-readings}))

(defn local-ts-devices-file [size file]
  (io/resource (format "ts-devices/small/devices_%s_%s.csv.gz"
                       (name size)
                       (case file
                         :readings "readings"
                         :device-info "device_info"))))

(defn gz-reader ^java.io.Reader [file]
  (-> (io/input-stream file)
      (GZIPInputStream.)
      (io/reader)))

(defn submit-ts-devices
  ([node]
   (submit-ts-devices node {}))

  ([node {:keys [size batch-size device-info-file readings-file],
                 :or {size :small
                      batch-size 1000
                      device-info-file (local-ts-devices-file size :device-info)
                      readings-file (local-ts-devices-file size :readings)}}]
   (assert device-info-file "Can't find device-info CSV")
   (assert readings-file "Can't find readings CSV")

   (with-open [device-info-rdr (gz-reader device-info-file)
               readings-rdr (gz-reader readings-file)]
     (let [device-infos (map device-info-csv->doc (csv/read-csv device-info-rdr))
           readings (map readings-csv->doc (csv/read-csv readings-rdr))
           [initial-readings rest-readings] (split-at (count device-infos) readings)]

       (->> (for [{:keys [time] :as doc} (concat (interleave device-infos initial-readings) rest-readings)]
              [:put-docs (cond-> {:into :docs}
                      time (assoc :valid-from time))
               doc])
            (partition-all batch-size)
            (reduce (fn [_acc tx-ops]
                      (xt/submit-tx node tx-ops))
                    nil))))))

(def query-recent-battery-temperatures
  ;; SELECT time, device_id, battery_temperature
  ;; FROM readings
  ;; WHERE battery_status = 'charging'
  ;; ORDER BY time DESC
  ;; LIMIT 10;

  '[:top {:limit 10}
    [:order-by [[time {:direction :desc}]]
     [:project [time device-id battery-temperature]
      [:scan [time device-id battery-temperature
              {battery-status (= battery-status "discharging")}]]]]])

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def query-busiest-low-battery-devices
  ;; SELECT time, readings.device_id, cpu_avg_1min,
  ;;        battery_level, battery_status, device_info.model
  ;; FROM readings
  ;;   JOIN device_info ON readings.device_id = device_info.device_id
  ;; WHERE battery_level < 33 AND battery_status = 'discharging'
  ;; ORDER BY cpu_avg_1min DESC, time DESC
  ;; LIMIT 5;

  '[:top {:limit 5}
    [:order-by [[cpu-avg-1min {:direction :desc}]
                [time {:direction :desc}]]
     [:join {device-id device-id}
      [:scan [device-id time cpu-avg-1min
              {battery-level (< battery-level 30)}
              {battery-status (= battery-status "discharging")}]]
      [:scan [device-id model]]]]])

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def query-min-max-battery-levels-per-hour
  ;; SELECT DATE_TRUNC(hour, time) "hour",
  ;;        MIN(battery_level) min_battery_level,
  ;;        MAX(battery_level) max_battery_level
  ;; FROM readings r
  ;; WHERE r.device_id IN (SELECT DISTINCT device_id FROM device_info
  ;;                       WHERE model = 'pinto' OR model = 'focus')
  ;; GROUP BY "hour"
  ;; ORDER BY "hour" ASC
  ;; LIMIT 12;

  '[:top {:limit 12}
    [:order-by [[hour]]
     [:group-by [hour
                 {min-battery-level (min battery-level)}
                 {max-battery-level (max battery-level)}]
      [:project [{hour (date-trunc "HOUR" time)}
                 battery-level]
       [:semi-join {device-id device-id}
        [:scan [device-id time battery-level]]
        [:scan [device-id {model (or (= model "pinto")
                                     (= model "focus"))}]]]]]]])

(comment
  (require 'dev '[xtdb.query-ra :as ra])

  (submit-ts-devices dev/node :small)

  (time
   (ra/query-ra query-recent-battery-temperatures {:node dev/node})))
