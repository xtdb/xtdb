(ns core2.ts-devices
  (:require [clojure.data.csv :as csv]
            [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [core2.core :as c2])
  (:import java.net.URL
           java.util.zip.GZIPInputStream))

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

(defn local-ts-devices-file [size file]
  (when-let [^URL this-ns (io/resource "core2/ts_devices.clj")]
    (when (= "file" (.getProtocol this-ns))
      (let [file (-> (io/file (io/as-file this-ns) "../../.."
                              "data/ts-devices"
                              (name size)
                              (format "devices_%s_%s.csv.gz"
                                      (name size)
                                      (case file
                                        :readings "readings"
                                        :device-info "device_info")))
                     .getCanonicalFile)]
        (when (.exists file)
          file)))))

(defn gz-reader ^java.io.Reader [file]
  (-> (io/input-stream file)
      (GZIPInputStream.)
      (io/reader)))

(defn submit-ts-devices
  ([tx-producer size]
   (submit-ts-devices tx-producer size {}))

  ([tx-producer size {:keys [batch-size], :or {batch-size 1000}}]
   (let [device-info-file (or (local-ts-devices-file size :device-info)
                              (throw (IllegalArgumentException.
                                      "Can't find device-info CSV")))
         readings-file (or (local-ts-devices-file size :readings)
                           (throw (IllegalArgumentException.
                                   "Can't find readings CSV")))]
     (with-open [device-info-rdr (gz-reader device-info-file)
                 readings-rdr (gz-reader readings-file)]
       (let [device-infos (map device-info-csv->doc (csv/read-csv device-info-rdr))
             readings (map readings-csv->doc (csv/read-csv readings-rdr))
             [initial-readings rest-readings] (split-at (count device-infos) readings)]

         @(->> (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                 {:op :put
                  :doc doc})
               (partition-all batch-size)
               (reduce (fn [_acc tx-ops]
                         (c2/submit-tx tx-producer tx-ops))
                       nil)))))))
