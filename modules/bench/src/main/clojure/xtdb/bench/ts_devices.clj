(ns xtdb.bench.ts-devices
  (:require [clojure.test :as t]
            [xtdb.bench :as b]
            [xtdb.bench.util :as bu]
            [xtdb.ts-devices :as tsd]
            [xtdb.util :as util])
  (:import (java.time Duration)
           (java.util AbstractMap)))

(defn download-file [size file-name]
  (let [tmp-file (bu/tmp-file-path (str "ts-devices." file-name) ".csv.gz")]
    (bu/download-s3-dataset-file (format "ts-devices/%s/devices_%s_%s.csv.gz"
                                         (name size) (name size) file-name)
                                 tmp-file)
    (.toFile tmp-file)))

(comment
  (def tmp-file (bu/tmp-file-path "ts-devices-test" "device_info"))
  (bu/download-s3-dataset-file "ts-devices/small/devices_small_device_info.csv.gz" tmp-file))

(defmethod b/->benchmark :ts-devices [_ {:keys [size seed] :or {seed 0}}]
  {:title "TS Devices Ingest"
   :seed seed,
   :tasks [{:t :do
            :stage :ingest
            :tasks [{:t :call
                     :stage :download-files
                     :f (fn [{:keys [^AbstractMap custom-state]}]
                          (.put custom-state :device-info-file (download-file size "device_info"))
                          (.put custom-state :readings-file (download-file size "readings")))}

                    {:t :call
                     :stage :submit-docs
                     :f (fn [{:keys [sut custom-state]}]
                          (tsd/submit-ts-devices sut {:device-info-file (get custom-state :device-info-file)
                                                      :readings-file (get custom-state :readings-file)}))}
                    {:t :call
                     :stage :sync
                     :f (fn [{:keys [sut]}] (b/sync-node sut (Duration/ofHours 5)))}

                    {:t :call
                     :stage :finish-block
                     :f (fn [{:keys [sut]}] (b/finish-block! sut))}]}]})

;; not intended to be run as a test - more for ease of REPL dev
(t/deftest ^:benchmark run-ts-devices
  (util/with-tmp-dirs #{node-tmp-dir}
    (-> (b/->benchmark :ts-devices {:size :small})
        (b/run-benchmark {:node-dir node-tmp-dir}))))
