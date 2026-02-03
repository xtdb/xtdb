(ns xtdb.bench.ts-devices
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.bench.util :as bu]
            [xtdb.ts-devices :as tsd]
            [xtdb.util :as util])
  (:import (java.time Duration)))

(defn download-file [size file-name]
  (let [tmp-file (bu/tmp-file-path (str "ts-devices." file-name) ".csv.gz")]
    (bu/download-s3-dataset-file (format "ts-devices/%s/devices_%s_%s.csv.gz"
                                         (name size) (name size) file-name)
                                 tmp-file)
    (.toFile tmp-file)))

(comment
  (def tmp-file (bu/tmp-file-path "ts-devices-test" "device_info"))
  (bu/download-s3-dataset-file "ts-devices/small/devices_small_device_info.csv.gz" tmp-file))

(defmethod b/cli-flags :ts-devices [_]
  [["-s" "--size SIZE" "Dataset size (small, med, big)"
    :parse-fn keyword
    :default :small]

   ["-h" "--help"]])

(defmethod b/->benchmark :ts-devices [_ {:keys [size seed] :or {size :small seed 0}}]
  (log/info {:size size :seed seed})
  {:title "TS Devices Ingest"
   :parameters {:size size}
   :seed seed,
   :->state #(do {:!state (atom {})})
   :tasks [{:t :do
            :stage :ingest
            :tasks [{:t :call
                     :stage :download-files
                     :setup? true
                     :f (fn [{:keys [!state]}]
                          (swap! !state assoc
                                 :device-info-file (download-file size "device_info")
                                 :readings-file (download-file size "readings")))}

                    {:t :call
                     :stage :submit-docs
                     :f (fn [{:keys [node !state]}]
                          (tsd/submit-ts-devices node {:device-info-file (get @!state :device-info-file)
                                                       :readings-file (get @!state :readings-file)}))}
                    {:t :call
                     :stage :sync
                     :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofHours 5)))}

                    {:t :call
                     :stage :finish-block
                     :f (fn [{:keys [node]}] (b/finish-block! node))}]}]})

;; not intended to be run as a test - more for ease of REPL dev
(t/deftest ^:benchmark run-ts-devices
  (util/with-tmp-dirs #{node-tmp-dir}
    (-> (b/->benchmark :ts-devices {:size :small})
        (b/run-benchmark {:node-dir node-tmp-dir}))))
