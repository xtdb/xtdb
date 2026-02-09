(ns xtdb.bench.cloud.scripts.tasks
  "Entry point for benchmark cloud scripts.
   CLI dispatch and help documentation."
  (:require [babashka.cli :as cli]
            [cheshire.core :as json]
            [clojure.string :as str]
            [xtdb.bench.cloud.scripts.charts :as charts]
            [xtdb.bench.cloud.scripts.k8s :as k8s]
            [xtdb.bench.cloud.scripts.log-parsing :as log-parsing]
            [xtdb.bench.cloud.scripts.summary :as summary]))

;; CLI Commands

(defn summarize-log
  "Summarize a benchmark log file.
   Usage: bb summarize-log [--format table|slack|github] <benchmark-type> <log-file>"
  [args]
  (let [{:keys [args opts]} (cli/parse-args args {:coerce {:format keyword}})
        [benchmark-type log-file-path & extra] args
        format (:format opts)]
    (when (seq extra)
      (throw (ex-info "Too many positional arguments supplied."
                      {:arguments args})))
    (when-not benchmark-type
      (throw (ex-info "Benchmark type is required."
                      {:arguments args})))
    (when-not log-file-path
      (throw (ex-info "Log file path is required."
                      {:arguments args})))
    (let [parsed-summary (log-parsing/load-summary benchmark-type log-file-path)]
      (case (:benchmark-type parsed-summary)
        "auctionmark" nil
        "clickbench" nil
        "tpch" (when (empty? (:query-stages parsed-summary))
                 (throw (ex-info "No query stages found in log file"
                                 {:benchmark-type "tpch"
                                  :log-file log-file-path})))
        "yakbench" (when (or (nil? (:profiles parsed-summary))
                             (empty? (:profiles parsed-summary)))
                     (throw (ex-info "No profile data found in log file"
                                     {:benchmark-type "yakbench"
                                      :log-file log-file-path})))
        "readings" (when (empty? (:query-stages parsed-summary))
                     (throw (ex-info "No query stages found in log file"
                                     {:benchmark-type "readings"
                                      :log-file log-file-path})))
        "tsbs-iot" (when (empty? (:ingest-stages parsed-summary))
                     (throw (ex-info "No ingest stages found in log file"
                                     {:benchmark-type "tsbs-iot"
                                      :log-file log-file-path})))
        "ingest-tx-overhead" (when (empty? (:batch-stages parsed-summary))
                               (throw (ex-info "No batch stages found in log file"
                                               {:benchmark-type "ingest-tx-overhead"
                                                :log-file log-file-path})))
        "patch" (when (empty? (:patch-stages parsed-summary))
                  (throw (ex-info "No patch stages found in log file"
                                  {:benchmark-type "patch"
                                   :log-file log-file-path})))
        "products" nil
        "ts-devices" nil
        "fusion" nil)
      (summary/render-summary parsed-summary {:format format}))))

(defn plot-benchmark-timeseries
  "Plot benchmark timeseries from Azure Log Analytics.
   Usage: bb plot-benchmark-timeseries [--scale-factor SF] <benchmark-type>"
  ([benchmark-type] (charts/plot-benchmark-timeseries benchmark-type))
  ([benchmark-type opts] (charts/plot-benchmark-timeseries benchmark-type opts)))

;; K8s Commands (new) - output JSON for workflow consumption

(defn- output-json
  "Print JSON to stdout for workflow consumption."
  [data]
  (println (json/generate-string data)))

(defn inspect-deployment
  "Inspect k8s deployment status. Outputs JSON.
   Usage: bb inspect-deployment [--namespace NS]"
  [args]
  (let [{:keys [opts]} (cli/parse-args args {})
        namespace (or (:namespace opts) "cloud-benchmark")]
    (output-json (k8s/inspect-deployment namespace))))

(defn check-immediate-failure
  "Check for immediate benchmark pod failures. Outputs JSON.
   Usage: bb check-immediate-failure [--namespace NS] [--attempts N] [--sleep-seconds N] [--required-running-seconds N]"
  [args]
  (let [{:keys [opts]} (cli/parse-args args {:coerce {:attempts :int
                                                      :sleep-seconds :int
                                                      :required-running-seconds :int}})
        params (cond-> {}
                 (:namespace opts) (assoc :namespace (:namespace opts))
                 (:attempts opts) (assoc :attempts (:attempts opts))
                 (:sleep-seconds opts) (assoc :sleep-seconds (:sleep-seconds opts))
                 (:required-running-seconds opts) (assoc :required-running-seconds (:required-running-seconds opts)))
        result (k8s/check-immediate-failure params)]
    (output-json result)
    (when (:failed result)
      (System/exit 1))))

(defn capture-benchmark-logs
  "Capture logs from benchmark pods. Outputs JSON.
   Usage: bb capture-benchmark-logs <job-name> [--namespace NS]"
  [args]
  (let [{:keys [args opts]} (cli/parse-args args {})
        [job-name & extra] args
        namespace (or (:namespace opts) "cloud-benchmark")]
    (when (seq extra)
      (throw (ex-info "Too many positional arguments supplied." {:arguments args})))
    (when-not job-name
      (throw (ex-info "Job name is required." {:arguments args})))
    (output-json (k8s/capture-benchmark-logs job-name {:namespace namespace}))))

(defn compute-grafana-time-range
  "Compute Grafana time range from log file. Outputs JSON.
   Usage: bb compute-grafana-time-range <log-path>"
  [args]
  (let [{:keys [args]} (cli/parse-args args {})
        [log-path & extra] args]
    (when (seq extra)
      (throw (ex-info "Too many positional arguments supplied." {:arguments args})))
    (when-not log-path
      (throw (ex-info "Log path is required." {:arguments args})))
    (output-json (k8s/compute-grafana-time-range log-path))))

(defn wait-for-benchmark-completion
  "Wait for benchmark job to complete. Outputs JSON.
   Usage: bb wait-for-benchmark-completion <job-name> [--namespace NS]"
  [args]
  (let [{:keys [args opts]} (cli/parse-args args {})
        [job-name & extra] args
        namespace (or (:namespace opts) "cloud-benchmark")]
    (when (seq extra)
      (throw (ex-info "Too many positional arguments supplied." {:arguments args})))
    (when-not job-name
      (throw (ex-info "Job name is required." {:arguments args})))
    (output-json (k8s/wait-for-benchmark-completion job-name {:namespace namespace}))))

(defn derive-benchmark-status
  "Derive benchmark status from job/pod states. Outputs JSON.
   Usage: bb derive-benchmark-status <job-name> [--namespace NS]"
  [args]
  (let [{:keys [args opts]} (cli/parse-args args {})
        [job-name & extra] args
        namespace (or (:namespace opts) "cloud-benchmark")]
    (when (seq extra)
      (throw (ex-info "Too many positional arguments supplied." {:arguments args})))
    (when-not job-name
      (throw (ex-info "Job name is required." {:arguments args})))
    (output-json (k8s/derive-benchmark-status job-name {:namespace namespace}))))

;; Help and Main

(defn help []
  (println "Usage: bb <command> [args...]")
  (println)
  (println "Log Analysis Commands:")
  (println "  summarize-log [--format table|slack|github] <benchmark-type> <log-file>")
  (println "      Print a benchmark summary. Default format is 'table'.")
  (println)
  (println "  plot-benchmark-timeseries [options] <benchmark-type>")
  (println "      Plot a benchmark timeseries chart from Azure Log Analytics.")
  (println "      Options: --scale-factor SF, --repo owner/repo, --branch branch")
  (println "      Supported: tpch, yakbench, auctionmark, readings, clickbench, tsbs-iot, ingest-tx-overhead, patch, products, ts-devices, fusion")
  (println)
  (println "Kubernetes Commands (output JSON):")
  (println "  inspect-deployment [--namespace NS]")
  (println "      Inspect k8s deployment and return status.")
  (println)
  (println "  check-immediate-failure [--namespace NS] [--attempts N]")
  (println "      Poll for immediate benchmark pod failures.")
  (println)
  (println "  capture-benchmark-logs <job-name> [--namespace NS]")
  (println "      Capture logs from all benchmark pods.")
  (println)
  (println "  compute-grafana-time-range <log-path>")
  (println "      Compute Grafana time range from benchmark log.")
  (println)
  (println "  wait-for-benchmark-completion <job-name> [--namespace NS]")
  (println "      Wait for benchmark job and pods to complete.")
  (println)
  (println "  derive-benchmark-status <job-name> [--namespace NS]")
  (println "      Derive final benchmark status from job/pod states.")
  (println)
  (println "Other:")
  (println "  help")
  (println "      Show this help message"))

(defn -main [& args]
  (if (empty? args)
    (help)
    (let [[command & rest-args] args]
      (try
        (case command
          "summarize-log"
          (let [output (summarize-log rest-args)]
            (print output)
            (flush))

          "plot-benchmark-timeseries"
          (let [{:keys [args opts]} (cli/parse-args rest-args {:coerce {:scale-factor :double}})
                [benchmark-type & extra] args
                {:keys [scale-factor repo branch]} opts]
            (when (seq extra)
              (throw (ex-info "Too many positional arguments supplied."
                              {:arguments rest-args})))
            (when-not benchmark-type
              (throw (ex-info "Benchmark type is required."
                              {:arguments rest-args})))
            (plot-benchmark-timeseries benchmark-type (cond-> {}
                                                        scale-factor (assoc :filter-value scale-factor)
                                                        repo (assoc :repo repo)
                                                        branch (assoc :branch branch))))

          ;; K8s commands
          "inspect-deployment" (inspect-deployment rest-args)
          "check-immediate-failure" (check-immediate-failure rest-args)
          "capture-benchmark-logs" (capture-benchmark-logs rest-args)
          "compute-grafana-time-range" (compute-grafana-time-range rest-args)
          "wait-for-benchmark-completion" (wait-for-benchmark-completion rest-args)
          "derive-benchmark-status" (derive-benchmark-status rest-args)

          "help" (help)

          (do
            (println (str "Unknown command: " command))
            (help)
            (System/exit 1)))
        (catch Exception e
          (binding [*out* *err*]
            (println "Error:" (.getMessage e))
            (when-let [data (ex-data e)]
              (println "Details:" data)))
          (System/exit 1))))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
