(ns xtdb.bench.tsbs
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.time :as time]
            [xtdb.tsbs :as tsbs]
            [xtdb.util :as util])
  (:import (java.time Duration LocalTime)))

(defmethod b/cli-flags :tsbs-iot [_]
  [[nil "--devices DEVICES"
    :id :devices
    :parse-fn parse-long]

   [nil "--timestamp-start TIMESTAMP_START"
    :id :timestamp-start
    :parse-fn time/->instant]

   [nil "--timestamp-end TIMESTAMP_END"
    :id :timestamp-end
    :parse-fn time/->instant]

   [nil "--file TXS_FILE"
    :id :txs-file
    :parse-fn util/->path]

   ["-h" "--help"]])

(defn- estimate-row-count
  "Estimate total rows based on devices and time range.
   Each device generates 1 reading + 1 diagnostic every 5 minutes (PT5M).
   Uses TSBS defaults (1 day) if timestamps not provided."
  [devices timestamp-start timestamp-end]
  (when devices
    (let [;; Use TSBS defaults: 2016-01-01 to 2016-01-02 (1 day)
          start (time/->instant (or timestamp-start #inst "2016-01-01T00:00:00Z"))
          end (time/->instant (or timestamp-end #inst "2016-01-02T00:00:00Z"))
          duration-millis (.toMillis (Duration/between start end))
          interval-millis (.toMillis #xt/duration "PT5M")
          readings-per-device (quot duration-millis interval-millis)
          total-readings (* devices readings-per-device)
          total-diagnostics total-readings
          total-rows (+ total-readings total-diagnostics)]
      {:devices devices
       :days (/ duration-millis (* 1000 60 60 24.0))
       :readings-per-device readings-per-device
       :total-readings total-readings
       :total-diagnostics total-diagnostics
       :total-rows total-rows})))

(defmethod b/->benchmark :tsbs-iot [_ {:keys [seed txs-file devices timestamp-start timestamp-end], :or {seed 0}, :as opts}]
  (when-let [est (and (not txs-file) (estimate-row-count devices timestamp-start timestamp-end))]
    (log/info (format "TSBS-IoT scale: %d devices × %.1f days = %,d total rows (%,d readings + %,d diagnostics)"
                      (:devices est) (:days est) (:total-rows est) (:total-readings est) (:total-diagnostics est))))

  {:title "TSBS IoT"
   :benchmark-type :tsbs-iot
   :seed seed
   :parameters {:seed seed
                :txs-file txs-file
                :devices devices
                :timestamp-start timestamp-start
                :timestamp-end timestamp-end}
   :tasks [{:t :do
            :stage :ingest
            :tasks [(letfn [(submit-txs [node txs]
                              (doseq [{:keys [ops system-time]} txs]
                                (when (Thread/interrupted) (throw (InterruptedException.)))

                                (when (= LocalTime/MIDNIGHT (.toLocalTime (time/->zdt system-time)))
                                  (log/debug "submitting" system-time))

                                (xt/submit-tx node ops {:system-time system-time})))]

                      (if txs-file
                        {:t :call
                         :stage :submit-docs
                         :f (fn [{:keys [node]}]
                              (tsbs/with-file-txs txs-file
                                (partial submit-txs node)))}

                        {:t :call
                         :stage :gen+submit-docs
                         :f (fn [{:keys [node]}]
                              (tsbs/make-gen)

                              (tsbs/with-generated-data (into {:seed seed
                                                               :use-case :iot,
                                                               :log-interval #xt/duration "PT5M"}
                                                              (if devices
                                                                ;; We pass devices as scale here to not confuse with scale-factor elsewhere
                                                                (assoc opts :scale devices)
                                                                opts))
                                (partial submit-txs node)))}))

                    {:t :call
                     :stage :sync
                     :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofHours 5)))}

                    {:t :call
                     :stage :finish-block
                     :f (fn [{:keys [node]}] (b/finish-block! node))}

                    {:t :call
                     :stage :compact
                     :f (fn [{:keys [node]}] (b/compact! node))}]}

           ;; TODO more queries
           {:t :call
            :stage :queries
            :f (fn [{:keys [node]}]
                 (let [readings-count (:row-count (first (xt/q node "SELECT COUNT(*) row_count FROM readings FOR ALL VALID_TIME")))
                       diagnostics-count (:row-count (first (xt/q node "SELECT COUNT(*) row_count FROM diagnostics FOR ALL VALID_TIME")))
                       total-count (+ readings-count diagnostics-count)]
                   (log/info (format "TSBS-IoT final counts: %,d readings + %,d diagnostics = %,d total rows"
                                     readings-count diagnostics-count total-count))))}]})

;; not intended to be run as a test - more for ease of REPL dev
(t/deftest ^:benchmark run-iot
  (util/with-tmp-dirs #{node-tmp-dir}
    (-> (b/->benchmark :tsbs-iot {:devices 40, :timestamp-start #inst "2020-01-01", :timestamp-end #inst "2020-01-07"})
        (b/run-benchmark {:node-dir node-tmp-dir}))))

(t/deftest ^:benchmark run-iot-from-file
  (util/with-tmp-dirs #{node-tmp-dir}
    (log/debug "tmp-dir:" node-tmp-dir)

    (-> (b/->benchmark :tsbs-iot {:txs-file #xt/path "/home/james/tmp/tsbs.transit.json"})
        (b/run-benchmark {:node-dir node-tmp-dir}))))
