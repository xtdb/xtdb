(ns ^:no-doc core2.cli
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [core2.node :as node]
            [core2.error :as err]
            [core2.util :as util])
  (:import java.io.File
           java.net.URL
           java.util.Map))

(defn- if-it-exists [^File f]
  (when (.exists f)
    f))

(defn- file-extension [^File f]
  (second (re-find #"\.(.+?)$" (.getName f))))

(def cli-options
  [["-f" "--file CONFIG_FILE" "Config file to load Core2 options from - EDN, JSON"
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

(defprotocol OptsSource
  (load-opts [src]))

(alter-meta! #'load-opts assoc :private true)

(defn- read-opts [src file-name]
  (cond
    (str/ends-with? file-name ".json") (json/read-str (slurp src) :key-fn keyword)
    (str/ends-with? file-name ".edn") (edn/read-string (slurp src))
    :else (throw (err/illegal-arg :unsupported-options-type
                                  {::err/message (format "Unsupported options type: '%s'" file-name)}))))

(extend-protocol OptsSource
  Map
  (load-opts [src] src)

  File
  (load-opts [src] (read-opts src (.getName src)))

  URL
  (load-opts [src] (read-opts src (.getFile src)))

  nil
  (load-opts [_] nil))

(defn parse-args [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (seq errors) {::errors errors}

      (:help options) {::help summary}

      :else (let [{:keys [file edn json]} options]
              {::node-opts (->> [(or file
                                     (some-> (io/file "core2.edn") if-it-exists)
                                     (some-> (io/file "core2.json") if-it-exists)
                                     (io/resource "core2.edn")
                                     (io/resource "core2.json"))

                                 json
                                 edn]
                                (map load-opts)
                                (apply merge-with merge))}))))

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
                                   (if (.isAlive main-thread)
                                     (do
                                       (log/warn "could not stop node cleanly after" shutdown-ms "ms, forcing exit")
                                       (.halt (Runtime/getRuntime) 1))

                                     (log/info "Node stopped."))))
                               "core2.shutdown-hook-thread"))
    shutdown?))

(defn start-node-from-command-line [args]
  (util/install-uncaught-exception-handler!)

  (let [{::keys [errors help node-opts]} (parse-args args)]
    (cond
      errors (binding [*out* *err*]
               (doseq [error errors]
                 (println error))
               (System/exit 1))

      help (println help)

      :else (with-open [_node (node/start-node node-opts)]
              (log/info "Node started")
              @(shutdown-hook-promise)))

    (shutdown-agents)))
