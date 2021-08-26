(ns ^:no-doc crux.cli
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [crux.api :as xt]
            [crux.io :as cio]
            [crux.node :as n]
            [clojure.data.json :as json])
  (:import java.io.File))

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
    :parse-fn json/read-str]

   ["-h" "--help"]])

(defn parse-args [args]
  (let [{:keys [options errors summary] :as parsed-opts} (cli/parse-opts args cli-options)]
    (cond
      (seq errors) {::errors errors}

      (:help options) {::help summary}

      :else (let [{:keys [file edn json]} options]
              {::node-opts [(or file
                                (some-> (io/file "xtdb.edn") if-it-exists)
                                (some-> (io/file "xtdb.json") if-it-exists)
                                (io/resource "xtdb.edn")
                                (io/resource "xtdb.json"))

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
              (with-open [node (xt/start-node node-opts)]
                (log/info "Node started")
                @(shutdown-hook-promise))))))
