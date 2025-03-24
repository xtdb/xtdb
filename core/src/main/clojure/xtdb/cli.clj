(ns ^:no-doc xtdb.cli
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.logging :as logging]
            [xtdb.migration :as mig]
            [xtdb.node :as xtn]
            [xtdb.pgwire :as pgw]
            [xtdb.util :as util])
  (:import java.io.File))

(defn- if-it-exists [^File f]
  (when (.exists f)
    f))

(defn read-env-var [env-var]
  (System/getenv (str env-var)))

(defn edn-read-string [edn-string]
  (edn/read-string {:readers {'env read-env-var}} edn-string))

(def cli-options
  [["-f" "--file CONFIG_FILE" "Config file to load XTDB options from - EDN, YAML"
    :parse-fn io/file
    :validate [if-it-exists "Config file doesn't exist"
               #(contains? #{"edn" "yaml"} (util/file-extension %)) "Config file must be .edn or .yaml"]]

   [nil "--migrate-from VERSION" "Migrates the database from the given version to the latest schema version"
    :id :migrate-from-version 
    :parse-fn parse-long]

   [nil "--playground" "Starts an XTDB playground on the default port"
    :id :playground-port
    :required false]

   [nil "--playground-port PORT" "Starts an XTDB playground"
    :id :playground-port
    :parse-fn parse-long]

   ["-h" "--help"]])

(defn edn-file->config-opts
  [^File f]
  (if (.exists f)
    (edn-read-string (slurp f))
    (throw (err/illegal-arg :opts-file-not-found
                            {::err/message (format "File not found: '%s'" (.getName f))}))))

(defn parse-args [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (seq errors) {::errors errors}

      (:help options) {::help summary}

      :else (let [{:keys [file playground-port migrate-from-version]} options]
              (cond
                (and file playground-port)
                {::errors ["Cannot specify both a config file and the playground option"]}

                (true? playground-port) {::playground-port 5432}
                (integer? playground-port) {::playground-port playground-port}

                :else {:migrate-from-version migrate-from-version
                       ::node-opts (let [file-extension (some-> file util/file-extension)
                                         yaml-file (if (= file-extension "yaml")
                                                     file
                                                     (or (some-> (io/file "xtdb.yaml") if-it-exists)
                                                         (some-> (io/resource "xtdb.yaml") (io/file))))
                                         edn-file-config (some-> (if (= file-extension "edn")
                                                                   file (or (some-> (io/file "xtdb.edn") if-it-exists)
                                                                            (some-> (io/resource "xtdb.edn") (io/file))))
                                                                 edn-file->config-opts)]
                                     (or yaml-file edn-file-config {}))})))))

(defn- shutdown-hook-promise []
  (let [main-thread (Thread/currentThread)
        !shutdown? (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (let [shutdown-ms 10000]
                                   (deliver !shutdown? true)
                                   (.join main-thread shutdown-ms)
                                   (if (.isAlive main-thread)
                                     (do
                                       (log/warn "could not stop node cleanly after" shutdown-ms "ms, forcing exit")
                                       (.halt (Runtime/getRuntime) 1))

                                     (log/info "Node stopped."))))
                               "xtdb.shutdown-hook-thread"))
    !shutdown?))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start-node-from-command-line [args]
  (util/install-uncaught-exception-handler!)

  (logging/set-from-env! (System/getenv))

  (try
    (let [{::keys [errors help migrate-from-version playground-port node-opts]} (parse-args args)]
      (cond
        errors (binding [*out* *err*]
                 (doseq [error errors]
                   (println error))
                 (System/exit 2))

        help (do
               (println help)
               (System/exit 0))

        migrate-from-version (System/exit (mig/migrate-from migrate-from-version node-opts))

        :else (util/with-open [_node (if playground-port
                                       (pgw/open-playground {:port playground-port})
                                       (xtn/start-node node-opts))]
                (log/info "Node started")
                ;; NOTE: This isn't registered until the node manages to start up
                ;; cleanly, so ctrl-c keeps working as expected in case the node
                ;; fails to start.
                @(shutdown-hook-promise)))

      (shutdown-agents))

    (catch Throwable t
      (shutdown-agents)
      (log/error t "Uncaught exception running XTDB")
      (System/exit 1))))
