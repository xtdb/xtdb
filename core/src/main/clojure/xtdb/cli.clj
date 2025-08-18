(ns ^:no-doc xtdb.cli
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.compactor.reset :as cr]
            [xtdb.error :as err]
            [xtdb.cli-help :as cli-help]
            [xtdb.logging :as logging]
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

(defn edn-file->config-opts
  [^File f]
  (if (.exists f)
    (edn-read-string (slurp f))
    (throw (err/incorrect :opts-file-not-found (format "File not found: '%s'" (.getName f))))))

(defn parse-args [args cli-spec]
  (let [{:keys [errors options summary] :as res} (cli/parse-opts args cli-spec)]
    (cond
      errors [:error errors]
      (:help options) [:help summary]
      :else [:success res])))

(defn- handling-arg-errors-or-help [[tag arg]]
  (case tag
    :error (binding [*out* *err*]
             (doseq [error arg]
               (println error))
             (System/exit 2))

    :help (do
            (println arg)
            (System/exit 0))

    :success arg))

(defn file->node-opts [file]
  (if-let [^File file (or file
                          (some-> (io/file "xtdb.yaml") if-it-exists)
                          (some-> (io/resource "xtdb.yaml") (io/file))
                          (some-> (io/file "xtdb.yml") if-it-exists)
                          (some-> (io/resource "xtdb.yml") (io/file))
                          (some-> (io/file "xtdb.edn") if-it-exists)
                          (some-> (io/resource "xtdb.edn") (io/file)))]
    (case (some-> file util/file-extension)
      ("yaml" "yml") file
      "edn" (edn-file->config-opts file)
      (throw (err/incorrect :config-file-not-found (format "File not found: '%s'" (.getName file)))))

    {}))

(defn- shutdown-hook-promise
  "NOTE: Don't register this until the node manages to start up cleanly, so that ctrl-c keeps working as expected in case the node fails to start. "
  []
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

(def config-file-opt
  ["-f" "--file CONFIG_FILE" "Config file to load XTDB options from - EDN, YAML"
   :id :file
   :parse-fn io/file
   :validate [if-it-exists "Config file doesn't exist"
              #(contains? #{"edn" "yaml"} (util/file-extension %)) "Config file must be .edn or .yaml"]])

(def compactor-cli-spec
  [config-file-opt
   ["-h" "--help"]])

(defn- start-compactor [args]
  (let [{{:keys [file]} :options} (-> (parse-args args compactor-cli-spec)
                                      (handling-arg-errors-or-help))]
    (log/info "Starting in compact-only mode...")

    (util/with-open [_node (xtn/start-compactor (file->node-opts file))]
      (log/info "Compactor started")
      @(shutdown-hook-promise))))

(def playground-cli-spec
  [["-p" "--port PORT"
    :id :port
    :parse-fn parse-long
    :default 5432]

   ["-h" "--help"]])

(defn- start-playground [args]
  (let [{{:keys [port]} :options} (-> (parse-args args playground-cli-spec)
                                      (handling-arg-errors-or-help))]
    (log/info "Starting in playground mode...")
    (util/with-open [_node (pgw/open-playground {:port port})]
      @(shutdown-hook-promise))))

(def node-cli-spec
  [config-file-opt
   ["-h" "--help"]])

(defn- start-node [args]
  (let [{{:keys [file]} :options} (-> (parse-args args node-cli-spec)
                                      (handling-arg-errors-or-help))]
    (util/with-open [_node (xtn/start-node (file->node-opts file))]
      @(shutdown-hook-promise))))

(def reset-compactor-cli-spec
  [config-file-opt
   [nil "--dry-run"
    "Lists files that would be deleted, without actually deleting them"
    :id :dry-run?]
   ["-h" "--help"]])

(defn- reset-compactor! [args]
  (let [{{:keys [dry-run? file]} :options, [db-name] :arguments} (-> (parse-args args reset-compactor-cli-spec)
                                                                     (handling-arg-errors-or-help))]
    (when (nil? db-name)
      (binding [*out* *err*]
        (println "Missing db-name: `reset-compactor <db-name> [opts]`")
        (System/exit 2)))
    (cr/reset-compactor! (file->node-opts file) db-name {:dry-run? dry-run?})))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start-node-from-command-line [[cmd & more-args :as args]]
  (util/install-uncaught-exception-handler!)

  (logging/set-from-env! (System/getenv))

  (try
    (case cmd
      "compactor" (start-compactor more-args)
      "playground" (start-playground more-args)
      "node" (start-node more-args)

      "reset-compactor" (do
                          (reset-compactor! more-args)
                          (System/exit 0))

      ("help" "-h" "--help") (do
                               (cli-help/print-help)
                               (System/exit 0))

      (if (or (empty? args) (str/starts-with? (first args) "-"))
        (start-node args)

        (do
          (cli-help/print-help)
          (System/exit 2))))

    (catch Throwable t
      (shutdown-agents)
      (log/error t "Uncaught exception running XTDB")
      (System/exit 1))))
