(ns crux.cli
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [crux.http-server :as srv]
            [crux.node :as n]
            [crux.config :as cc])
  (:import java.io.Closeable))

(def default-options
  {:crux.node/topology :crux.kafka/topology})

(def cli-options
  [;; Kafka
   ["-p" "--properties-file PROPERTIES_FILE" "Properties file to load Crux options from"
    :parse-fn #(cc/load-properties %)]

   ;; HTTP
   ["-s" "--server-port SERVER_PORT" "Port on which to run the HTTP server"
    :default srv/default-server-port
    :parse-fn #(Long/parseLong %)]

   ;; Extra
   ["-x" "--extra-edn-options EDN_OPTIONS" "Extra options as an quoted EDN map."
    :default nil
    :parse-fn edn/read-string]

   ["-h" "--help"]])

;; NOTE: This isn't registered until the node manages to start up
;; cleanly, so ctrl-c keeps working as expected in case the node
;; fails to start.
(defn- shutdown-hook-promise []
  (let [main-thread (Thread/currentThread)
        shutdown? (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (let [shutdown-ms 10000]
                                   (deliver shutdown? true)
                                   (shutdown-agents)
                                   (.join main-thread shutdown-ms)
                                   (when (.isAlive main-thread)
                                     (log/warn "could not stop node cleanly after" shutdown-ms "ms, forcing exit")
                                     (.halt (Runtime/getRuntime) 1))))
                               "crux.shutdown-hook-thread"))
    shutdown?))

(defn- options->table [options]
  (with-out-str
    (pp/print-table (for [[k v] options]
                      {:key k :value v}))))

(defn start-node-from-command-line [args]
  (n/install-uncaught-exception-handler!)
  (let [{:keys [options
                errors
                summary]} (cli/parse-opts args cli-options)
        {:keys [server-port properties-file extra-edn-options]} options
        options (merge default-options
                       {:server-port server-port}
                       extra-edn-options
                       properties-file)
        {:keys [version
                revision]} (n/crux-version)]
    (cond
      (:help options)
      (println summary)

      errors
      (binding [*out* *err*]
        (doseq [error errors]
          (println error))
        (System/exit 1))

      :else
      (do (log/infof "Crux version: %s revision: %s" version revision)
          (log/info "options:" (options->table options))
          (with-open [node (n/start options)
                      http-server ^Closeable (srv/start-http-server node options)]
            @(shutdown-hook-promise))))))
