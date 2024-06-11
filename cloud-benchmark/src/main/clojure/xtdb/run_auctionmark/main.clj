(ns xtdb.run-auctionmark.main
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.stacktrace :as st]
            [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.bench.auctionmark :as am]
            [xtdb.bench.measurement :as bm]
            [xtdb.node :as xtn]
            [clojure.java.io :as io]))

(def run-duration (or (System/getenv "AUCTIONMARK_DURATION") "PT24H"))
(def scale-factor (or (some-> (System/getenv "AUCTIONMARK_SCALE_FACTOR") (Float/parseFloat)) 0.1))
(def load-phase (if-let [lp (System/getenv "AUCTIONMARK_LOAD_PHASE")] 
                  (Boolean/parseBoolean lp)
                  true))
(def bench-secrets
  (some-> (System/getenv "BENCH_SECRETS")
          (json/parse-string)))

(def slack-url
  (or (some-> bench-secrets
              (get "SLACK_WEBHOOK_URL")
              (not-empty))
      (some-> (System/getenv "SLACK_WEBHOOK_URL")
              (not-empty))))

(defn send-error-to-slack [error]
  (when slack-url
    (let [error-message (format
                         ":x: Error thrown within Auctionmark: `%s`! \n\n*Stack Trace:*\n ```%s```"
                         (.getMessage error) 
                         (with-out-str (st/print-stack-trace error 15)))]
      (http/post slack-url {:headers {"Content-Type" "application/json"}
                            :body (json/generate-string {:text error-message})}))))

(defn send-success-to-slack [{:keys [duration scale-factor load-phase]}]
  (when slack-url
    (let [success-message (format
                           ":white_check_mark: Auctionmark successfully ran for `%s`! \n\n*Scale Factor*: `%s` \n*Load-Phase*: `%s`"
                           duration scale-factor load-phase) ]
      (http/post slack-url {:headers {"Content-Type" "application/json"}
                            :body (json/generate-string {:text success-message})}))))

(defn -main [] 
  (let [am-config {:seed 0
                   :threads 8
                   :duration run-duration
                   :scale-factor scale-factor
                   :load-phase load-phase
                   :sync false}
        benchmark (am/benchmark am-config)
        benchmark-fn (b/compile-benchmark benchmark bm/wrap-task)]
    (log/info "Running Auctionmark with the following config... \n" am-config)
    (log/info "Starting node with config... \n" (slurp (io/file "node-config.yaml"))) 
    (try
      (with-open [node (xtn/start-node (io/file "node-config.yaml"))]
        (benchmark-fn node)
        (send-success-to-slack am-config))
      
      (catch Exception e
        (log/error "Error running Auctionmark: " (.getMessage e))
        (send-error-to-slack e)
        (throw e)))))
