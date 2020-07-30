(ns ^:no-doc crux.cli
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [crux.node :as n]
            [crux.config :as cc]
            [crux.io :as cio]
            [clojure.java.io :as io]
            [crux.api :as crux]
            [cheshire.core :as json])
  (:import (java.io Closeable File)))

(defn- if-it-exists [^File f]
  (when (.exists f)
    f))

(defn- file-extension [^File f]
  (second (re-find #"\.(.+?)$" (.getName f))))

(def cli-options
  [["-f" "--file CONFIG_FILE" "Config file to load Crux options from - EDN, JSON"
    :parse-fn io/file
    :validate [if-it-exists "Config file doesn't exist"
               #(contains? #{"edn" "json"} (file-extension %)) "Config file must be .edn or .json"]]

   ["-e" "--edn EDN" "Options as EDN."
    :default nil
    :parse-fn edn/read-string]

   ["-j" "--json JSON" "Options as JSON."
    :default nil
    :parse-fn edn/read-string]

   ["-h" "--help"]])

(defn load-edn [f]
  (edn/read-string (slurp f)))

(defn load-json [f]
  (json/decode (slurp f)))

(defn parse-args [args]
  (let [{:keys [options errors summary] :as parsed-opts} (cli/parse-opts args cli-options)]
    (cond
      (seq errors) {::errors errors}

      (:help options) {::help summary}

      :else (let [{:keys [file edn json]} options]
              {::node-opts [(or (when file
                                  (case (file-extension file)
                                    "edn" (load-edn file)
                                    "json" (load-json file)))
                                (some-> (io/file "crux.edn") if-it-exists (load-edn))
                                (some-> (io/file "crux.json") if-it-exists (load-json))
                                (some-> (io/resource "crux.edn") (load-edn))
                                (some-> (io/resource "crux.json") (load-json)))

                            json
                            edn]}))))

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
              (with-open [node (n/start node-opts)]
                (log/info "Node started")
                @(shutdown-hook-promise))))))
