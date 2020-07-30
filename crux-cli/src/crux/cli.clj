(ns ^:no-doc crux.cli
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [crux.node :as n]
            [crux.config :as cc]
            [crux.io :as cio]
            [clojure.java.io :as io]
            [crux.api :as crux])
  (:import (java.io Closeable File)))

(def default-options
  {:crux.node/topology '[crux.standalone/topology crux.http-server/module]
   :crux.kv/db-dir "db-dir"})

(defn- if-it-exists [^File f]
  (when (.exists f)
    f))

(def cli-options
  [["-e" "--edn-file EDN_FILE" "EDN file to load Crux options from"
    :parse-fn io/file
    :validate [if-it-exists "EDN file doesn't exist"]]

   ["-p" "--properties-file PROPERTIES_FILE" "Properties file to load Crux options from"
    :parse-fn io/file
    :validate [if-it-exists "Properties file doesn't exist"]]

   ["-x" "--extra-edn-options EDN_OPTIONS" "Extra options as an quoted EDN map."
    :default nil
    :parse-fn edn/read-string]

   ["-h" "--help"]])

(defn parse-args [args]
  (let [{:keys [options errors summary] :as parsed-opts} (cli/parse-opts args cli-options)]
    (cond
      (seq errors) {::errors errors}

      (:help options) {::help summary}

      :else (let [{:keys [edn-file properties-file extra-edn-options]} options]
              {::node-opts (merge default-options

                                  (or (some-> edn-file (cc/load-edn))
                                      (some-> properties-file (cc/load-properties))
                                      (some-> (io/file "crux.edn") if-it-exists (cc/load-edn))
                                      (some-> (io/file "crux.properties") if-it-exists (cc/load-properties))
                                      (some-> (io/resource "crux.edn") (cc/load-edn))
                                      (some-> (io/resource "crux.properties") (cc/load-properties)))

                                  extra-edn-options)}))))

(defn- options->table [options]
  (with-out-str
    (pp/print-table (for [[k v] options]
                      {:key k :value v}))))

(defn- shutdown-hook-promise []
  ;; NOTE: This isn't registered until the node manages to start up
  ;; cleanly, so ctrl-c keeps working as expected in case the node
  ;; fails to start.
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

(defn start-node-from-command-line [args]
  (cio/install-uncaught-exception-handler!)

  (let [{::keys [errors help node-opts]} (parse-args args)]
    (cond
      errors (binding [*out* *err*]
               (doseq [error errors]
                 (println error))
               (System/exit 1))

      help (println help)

      :else (let [{:keys [version revision]} n/crux-version]
              (log/infof "Crux version: %s revision: %s" version revision)
              (log/info "options:" (options->table node-opts))
              (with-open [node (crux/start-node node-opts)]
                @(shutdown-hook-promise))))))
