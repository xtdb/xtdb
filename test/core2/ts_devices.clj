(ns core2.ts-devices
  (:require [clojure.instant :as inst]
            [clojure.string :as str]))

(defn device-info-csv->doc [[device-id api-version manufacturer model os-name]]
  {:_id (str "device-info-" device-id)
   :api-version api-version
   :manufacturer manufacturer
   :model model
   :os-name os-name})

(defn readings-csv->doc [[time device-id battery-level battery-status
                           battery-temperature bssid
                           cpu-avg-1min cpu-avg-5min cpu-avg-15min
                           mem-free mem-used rssi ssid]]
  {:_id (str "reading-" device-id)
   :time (inst/read-instant-date
          (-> time
              (str/replace " " "T")
              (str/replace #"-(\d\d)$" ".000-$1:00")))
   :device-id (str "device-info-" device-id)
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
   :ssid ssid})
