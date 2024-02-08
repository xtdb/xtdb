(ns xtdb.bench.ts-devices
  (:require [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.bench.xtdb2 :as bxt]
            [xtdb.ts-devices :as tsd]
            [xtdb.util :as util])
  (:import (java.time Duration InstantSource)
           (java.util AbstractMap)))

(def cli-arg-spec
  [[nil "--size <small|med|big>" "Size of ts-devices files to use"
    :id :size
    :default :small
    :parse-fn keyword
    :validate-fn (comp boolean #{:small :med :big})]
   b/report-file])

(defn download-file [size file-name]
  (let [tmp-file (b/tmp-file-path (str "ts-devices." file-name) ".csv.gz")]
    (b/download-s3-dataset-file (format "ts-devices/%s/devices_%s_%s.csv.gz"
                                        (name size) (name size) file-name)
                                tmp-file)
    (.toFile tmp-file)))

(comment
  (def tmp-file (b/tmp-file-path "ts-devices-test" "device_info"))
  (b/download-s3-dataset-file "ts-devices/small/devices_small_device_info.csv.gz" tmp-file))


(defn benchmark [{:keys [size seed] :or {seed 0}}]
  {:title "TS Devices Ingest"
   :seed seed
   :tasks
   [{:t :do
     :stage :ingest
     :tasks [{:t :do
              :stage :download-files
              :tasks [{:t :call :f (fn [{:keys [^AbstractMap custom-state]}]
                                     (.put custom-state :device-info-file (download-file size "device_info"))
                                     (.put custom-state :readings-file (download-file size "readings")))}]}
             {:t :do
              :stage :submit-docs
              :tasks [{:t :call :f (fn [{:keys [sut custom-state]}]
                                     (tsd/submit-ts-devices sut {:device-info-file (get custom-state :device-info-file)
                                                                 :readings-file (get custom-state :readings-file)}))}]}
             {:t :do
              :stage :sync
              :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/sync-node sut (Duration/ofHours 5)))}]}
             {:t :do
              :stage :finish-chunk
              :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/finish-chunk! sut))}]}]}]})

(defn -main [& args]
  (try
    (let [{:keys [report size] :as opts} (or (b/parse-args cli-arg-spec args)
                                             (System/exit 1))]
      (log/info "Opts: " (pr-str opts))
      (spit report
            (util/with-tmp-dirs #{node-tmp-dir}
              (bxt/run-benchmark
               {:node-opts {:node-dir node-tmp-dir
                            :instant-src (InstantSource/system)}
                :benchmark-type :ts-devices
                :benchmark-opts {:size size}}))))
    (catch Exception e
      (.printStackTrace e)
      (System/exit 1))

    (finally
      (shutdown-agents))))

(comment
  (util/with-tmp-dirs #{node-tmp-dir}
    (def report-ts-devices
      (bxt/run-benchmark
       {:node-opts {:node-dir node-tmp-dir
                    :instant-src (InstantSource/system)}
        :benchmark-type :ts-devices
        :benchmark-opts {:size :med}})))

  (xtdb.bench.report/show-html-report
   (xtdb.bench.report/vs
    "core2-tpch"
    report-ts-devices)))
