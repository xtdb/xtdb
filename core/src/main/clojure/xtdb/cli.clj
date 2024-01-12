(ns ^:no-doc xtdb.cli
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log] 
            [xtdb.config :as config] 
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import java.io.File))

(defn- if-it-exists [^File f]
  (when (.exists f)
    f))

(def cli-options
  [["-f" "--file CONFIG_FILE" "Config file to load XTDB options from - EDN, JSON, YAML"
    :parse-fn io/file
    :validate [if-it-exists "Config file doesn't exist"
               #(contains? #{"edn" "json" "yaml"} (util/file-extension %)) "Config file must be .edn, .json or .yaml"]]

   ["-e" "--edn EDN" "Options as EDN."
    :default nil
    :parse-fn config/edn-read-string]

   ["-j" "--json JSON" "Options as JSON."
    :default nil
    :parse-fn config/json-read-string]

   ["-h" "--help"]])

(defn parse-args [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (seq errors) {::errors errors}

      (:help options) {::help summary}

      :else (let [{:keys [file edn json]} options]
              {::node-opts (let [config-file (or file
                                                 (some-> (io/file "xtdb.edn") if-it-exists)
                                                 (some-> (io/file "xtdb.json") if-it-exists)
                                                 (some-> (io/file "xtdb.yaml") if-it-exists) 
                                                 (io/resource "xtdb.edn")
                                                 (io/resource "xtdb.json")
                                                 (io/resource "xtdb.yaml"))
                                 config-from-file (some-> config-file config/file->config-opts)]
                             (or config-from-file json edn))}))))

(defn- shutdown-hook-promise []
  (let [main-thread (Thread/currentThread)
        shutdown? (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (let [shutdown-ms 10000]
                                   (deliver shutdown? true)
                                   (shutdown-agents)
                                   (.join main-thread shutdown-ms)
                                   (if (.isAlive main-thread)
                                     (do
                                       (log/warn "could not stop node cleanly after" shutdown-ms "ms, forcing exit")
                                       (.halt (Runtime/getRuntime) 1))

                                     (log/info "Node stopped."))))
                               "xtdb.shutdown-hook-thread"))
    shutdown?))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start-node-from-command-line [args]
  (util/install-uncaught-exception-handler!)

  (let [{::keys [errors help node-opts]} (parse-args args)]
    (cond
      errors (binding [*out* *err*]
               (doseq [error errors]
                 (println error))
               (System/exit 1))

      help (println help)

      :else (with-open [_node (xtn/start-node node-opts)]
              (log/info "Node started")
              ;; NOTE: This isn't registered until the node manages to start up
              ;; cleanly, so ctrl-c keeps working as expected in case the node
              ;; fails to start.
              @(shutdown-hook-promise)))

    (shutdown-agents)))
