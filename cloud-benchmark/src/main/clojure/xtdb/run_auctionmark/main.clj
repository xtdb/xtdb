(ns xtdb.run-auctionmark.main
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.java.io :as io]
            [clojure.stacktrace :as st]
            [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.bench.auctionmark :as am]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import [clojure.lang ExceptionInfo]))

(def run-duration (or (System/getenv "AUCTIONMARK_DURATION") "PT24H"))
(def scale-factor (or (some-> (System/getenv "AUCTIONMARK_SCALE_FACTOR") (Float/parseFloat)) 0.1))
(def load-phase (if-let [lp (System/getenv "AUCTIONMARK_LOAD_PHASE")]
                  (Boolean/parseBoolean lp)
                  true))
(def load-phase-only? (if-let [lp (System/getenv "AUCTIONMARK_LOAD_PHASE_ONLY")]
                        (Boolean/parseBoolean lp)
                        false))

(def bench-secrets
  (some-> (System/getenv "BENCH_SECRETS")
          (json/parse-string)))

(def slack-url
  (or (some-> bench-secrets
              (get "SLACK_WEBHOOK_URL")
              (not-empty))
      (some-> (System/getenv "SLACK_WEBHOOK_URL")
              (not-empty))))

(def platform (or (System/getenv "CLOUD_PLATFORM_NAME") "Local"))

(def config-file (or (System/getenv "CONFIG_FILE") "node-config.yaml"))

(defn send-message-to-slack [message]
  (when slack-url
    (http/post slack-url {:headers {"Content-Type" "application/json"}
                          :body (json/generate-string {:text message})})))

;; If ex-info contains `system` suggests integrant runtime error
;; - get the cause instead, as that's more useful to us
;; See https://github.com/weavejester/integrant/blob/master/src/integrant/core.cljc#L270
(defn ex-info-handle-integrant-runtime-error [ex]
  (let [{:keys [system]} (ex-data ex)]
    (if system (ex-cause ex) ex)))

(defn send-error-to-slack [e]
  (when slack-url
    (let [^Exception error (if (instance? ExceptionInfo e)
                             (ex-info-handle-integrant-runtime-error e)
                             e)
          error-message (format
                         ":x: Error thrown within (%s) Auctionmark: *%s*! \n\n*Stack Trace:*\n ```%s```"
                         platform
                         (.getMessage error)
                         (with-out-str (st/print-cause-trace error 15)))]
      (send-message-to-slack error-message))))

(defn run-load-phase-only [node]
  (let [am-config {:seed 0
                   :scale-factor scale-factor}
        load-phase-bench (am/load-phase-only am-config)
        load-phase-fn (b/compile-benchmark load-phase-bench)]
    (log/info "Running Load Phase with the following config... \n" am-config)
    (try
      (binding [bm/*registry* (util/component node :xtdb.metrics/registry)]
        (load-phase-fn node))
      (send-message-to-slack
       (format ":white_check_mark: (%s) Auctionmark Load Phase successfully ran for *Scale Factor*: `%s`"  platform scale-factor))

      (catch Exception e
        (log/error "Error running Auctionmark Load Phase: " (.getMessage e))
        (send-error-to-slack e)
        (throw e)))))

(defn run-full-auctionmark [node]
  (let [am-config {:seed 0
                   :threads 8
                   :duration run-duration
                   :scale-factor scale-factor
                   :load-phase load-phase
                   ;; May as well sync data after running the threads prior to closing the node to check for oddities
                   :sync true}
        benchmark (am/benchmark am-config)
        benchmark-fn (b/compile-benchmark benchmark)]
    (log/info "Running Auctionmark Benchmark with the following config... \n" am-config)
    (try
      (binding [bm/*registry* (util/component node :xtdb.metrics/registry)]
        (benchmark-fn node))
      (send-message-to-slack
       (format
        ":white_check_mark: (%s) Auctionmark successfully ran for *%s*! \n\n*Scale Factor*: `%s` \n*Load-Phase*: `%s`" platform run-duration scale-factor load-phase))

      (catch Exception e
        (log/error "Error running Auctionmark: " (.getMessage e))
        (send-error-to-slack e)
        (throw e)))))

(defn -main []
  (log/info "Starting node with config... \n" (slurp (io/file config-file)))
  (with-open [node (xtn/start-node (io/file config-file))]
    (if load-phase-only?
      (run-load-phase-only node)
      (run-full-auctionmark node))))
