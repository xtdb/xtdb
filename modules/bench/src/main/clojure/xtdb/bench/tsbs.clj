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
  [[nil "--scale SCALE"
    :parse-fn parse-long]

   ["-f" "--file TXS_FILE"
    :id :txs-file
    :parse-fn util/->path]

   ["-h" "--help"]])

(defmethod b/->benchmark :tsbs-iot [_ {:keys [seed txs-file], :or {seed 0}, :as opts}]
  {:title "TSBS IoT"
   :benchmark-type :tsbs-iot
   :seed seed,
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

                              (tsbs/with-generated-data (into {:use-case :iot,
                                                               :log-interval #xt/duration "PT5M"}
                                                              opts)
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
                 (log/info "readings:" (:row-count (first (xt/q node "SELECT COUNT(*) row_count FROM readings FOR ALL VALID_TIME"))))
                 (log/info "diagnostics:" (:row-count (first (xt/q node "SELECT COUNT(*) row_count FROM diagnostics FOR ALL VALID_TIME")))))}]})

;; not intended to be run as a test - more for ease of REPL dev
(t/deftest ^:benchmark run-iot
  (util/with-tmp-dirs #{node-tmp-dir}
    (-> (b/->benchmark :tsbs-iot {:scale 40, :timestamp-start #inst "2020-01-01", :timestamp-end #inst "2020-01-07"})
        (b/run-benchmark {:node-dir node-tmp-dir}))))

(t/deftest ^:benchmark run-iot-from-file
  (util/with-tmp-dirs #{node-tmp-dir}
    (log/debug "tmp-dir:" node-tmp-dir)

    (-> (b/->benchmark :tsbs-iot {:txs-file #xt/path "/home/james/tmp/tsbs.transit.json"})
        (b/run-benchmark {:node-dir node-tmp-dir}))))
