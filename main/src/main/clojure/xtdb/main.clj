(ns xtdb.main
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            clojure.pprint.dispatch
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.logging :as logging]
            [xtdb.util :as util])
  (:import java.io.File))

(defn version-string []
  (let [version (some-> (System/getenv "XTDB_VERSION")
                        str/trim
                        not-empty)
        git-sha (some-> (System/getenv "GIT_SHA")
                        str/trim
                        not-empty
                        (subs 0 7))]
    (str "XTDB"
         (if version (str " " version) " 2.x")
         (when git-sha (str " [" git-sha "]")))))

(defn print-help []
  (println (str "--- " (version-string) " ---"))
  (newline)
  (println "XTDB has several top-level commands to choose from:")
  (println " * `node` (default, can be omitted): starts an XT node")
  (println " * `compactor`: runs a compactor-only node")
  (println " * `playground`: starts a 'playground', an in-memory node which accepts any database name, creating it if required")
  (println " * `reset-compactor <db-name>`: resets the compacted files on the given node.")
  (println " * `export-snapshot <db-name>`: exports a consistent snapshot of object storage for the given database.")
  (println " * `tx-sink`: runs a node which replicates the transaction log to an external log")
  (newline)
  (println "For more information about any command, run `<command> --help`, e.g. `playground --help`"))

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

    (util/with-open [_node ((requiring-resolve 'xtdb.node/start-compactor) (file->node-opts file))]
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
    (util/with-open [_node ((requiring-resolve 'xtdb.pgwire/open-playground) {:port port})]
      (log/info "Playground started")
      @(shutdown-hook-promise))))

(def node-cli-spec
  [config-file-opt
   ["-h" "--help"]])

(defn- start-node [args]
  (let [{{:keys [file]} :options} (-> (parse-args args node-cli-spec)
                                      (handling-arg-errors-or-help))]
    (util/with-open [_node ((requiring-resolve 'xtdb.node/start-node) (file->node-opts file))]
      (log/info "Node started")
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

    ((requiring-resolve 'xtdb.compactor.reset/reset-compactor!) (file->node-opts file) db-name {:dry-run? dry-run?})))

(def tx-sink-cli-spec
  [config-file-opt
   ["-h" "--help"]])

(defn- tx-sink! [args]
  (let [{{:keys [file]} :options} (-> (parse-args args tx-sink-cli-spec)
                                      (handling-arg-errors-or-help))]
    (util/with-open [_node ((requiring-resolve 'xtdb.tx-sink.main/open!) (file->node-opts file))]
      (log/info "Tx Sink node started")
      @(shutdown-hook-promise))))

(def export-snapshot-cli-spec
  [config-file-opt
   [nil "--dry-run"
    "Lists files that would be copied, without actually copying them"
    :id :dry-run?]
   ["-h" "--help"]])

(defn- export-snapshot! [args]
  (let [{{:keys [dry-run? file]} :options, [db-name] :arguments} (-> (parse-args args export-snapshot-cli-spec)
                                                                     (handling-arg-errors-or-help))]
    (when (nil? db-name)
      (binding [*out* *err*]
        (println "Missing db-name: `export-snapshot <db-name> [opts]`")
        (System/exit 2)))

    ((requiring-resolve 'xtdb.export/export-snapshot!) (file->node-opts file) db-name {:dry-run? dry-run?})))

(def read-arrow-file-cli-spec
  [["-h" "--help"]])

(defn read-arrow-file [args]
  (let [{{:keys [help]} :options} (-> (parse-args args read-arrow-file-cli-spec)
                                      (handling-arg-errors-or-help))]
    (if help
      (do
        (println "Usage: read-arrow-file <file>")
        (System/exit 0))

      (if-let [file (first args)]
        (binding [pp/*print-right-margin* 120]
          ((requiring-resolve 'xtdb.arrow/with-arrow-file) file pp/pprint))

        (binding [*out* *err*]
          (println "Usage: `read-arrow-file <file>`")
          (System/exit 2))))))

(defn read-arrow-stream-file [args]
  (let [{{:keys [help]} :options} (-> (parse-args args read-arrow-file-cli-spec)
                                      (handling-arg-errors-or-help))]
    (if help
      (do
        (println "Usage: read-arrow-stream-file <file>")
        (System/exit 0))

      (if-let [file (first args)]
        (binding [pp/*print-right-margin* 120]
          (pp/pprint ((requiring-resolve 'xtdb.arrow/read-arrow-stream-file) file)))

        (binding [*out* *err*]
          (println "Usage: `read-arrow-stream-file <file>`")
          (System/exit 2))))))

(def read-hash-trie-file-cli-spec
  [["-h" "--help"]])

(defn read-hash-trie-file [args]
  (let [{{:keys [help]} :options} (-> (parse-args args read-hash-trie-file-cli-spec)
                                      (handling-arg-errors-or-help))]
    (if help
      (do
        (println "Usage: read-hash-trie-file <file>")
        (System/exit 0))

      (if-let [file (first args)]
        (binding [pp/*print-right-margin* 120]
          (pp/pprint ((requiring-resolve 'xtdb.arrow/read-hash-trie-file) file)))

        (binding [*out* *err*]
          (println "Usage: `read-hash-trie-file <file>")
          (System/exit 2))))))

(def read-table-block-file-cli-spec
  [["-h" "--help"]])

(defn read-table-block-file [args]
  (let [{{:keys [help]} :options} (-> (parse-args args read-table-block-file-cli-spec)
                                      (handling-arg-errors-or-help))]
    (if help
      (do
        (println "Usage: read-table-block-file <file>")
        (System/exit 0))

      (if-let [file (first args)]
        (binding [pp/*print-right-margin* 120]
          (pp/pprint ((requiring-resolve 'xtdb.pbuf/read-table-block-file) file)))

        (binding [*out* *err*]
          (println "Usage: `read-table-block-file <file>`")
          (System/exit 2))))))

(defn -main [& args]
  (binding [*out* *err*]
    (println (str "Starting " (version-string) " ...")))

  (util/install-uncaught-exception-handler!)

  (logging/set-from-env! (System/getenv))

  (let [[cmd & more-args] args]
    (try
      (case cmd
        "compactor" (start-compactor more-args)
        "playground" (start-playground more-args)
        "node" (start-node more-args)

        "reset-compactor" (do
                            (reset-compactor! more-args)
                            (System/exit 0))

        "tx-sink" (do
                    (tx-sink! more-args)
                    (System/exit 0))

        "export-snapshot" (do
                            (export-snapshot! more-args)
                            (System/exit 0))

        "read-arrow-file" (do
                            (read-arrow-file more-args)
                            (System/exit 0))

        "read-arrow-stream-file" (do
                                   (read-arrow-stream-file more-args)
                                   (System/exit 0))

        "read-hash-trie-file" (do
                                (read-hash-trie-file more-args)
                                (System/exit 0))

        "read-table-block-file" (do
                                  (read-table-block-file more-args)
                                  (System/exit 0))

        ("help" "-h" "--help") (do
                                 (print-help)
                                 (System/exit 0))

        (if (or (empty? args) (str/starts-with? (first args) "-"))
          (start-node args)

          (do
            (print-help)
            (System/exit 2))))

      (catch Throwable t
        (shutdown-agents)
        (log/error t "Uncaught exception running XTDB")
        (System/exit 1)))))

