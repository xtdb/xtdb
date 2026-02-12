(ns xtdb.bench
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.log :as xt-log]
            [xtdb.logging :as logging]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (com.google.common.collect MinMaxPriorityQueue)
           (io.micrometer.core.instrument Meter Timer)
           (java.io File Writer)
           (java.lang.management ManagementFactory)
           (java.time Clock Duration InstantSource)
           java.time.Duration
           (java.util Comparator Random)
           (java.util.concurrent ExecutionException Executors TimeUnit)
           (java.util.concurrent.atomic AtomicLong)
           (oshi SystemInfo)))

;; Track whether we're in shutdown mode to suppress error logging during cleanup
(defonce ^:private shutting-down? (atom false))

(extend-protocol json/JSONWriter
  Duration
  (-write [^Duration duration out _options]
    (let [^Appendable out out]
      (.append out \")
      (.append out (.toString duration))
      (.append out \"))))

(def ^:private timeout-sentinel ::benchmark-timeout)

(defn- normalize-timeout ^Duration [^Duration duration]
  (when (and duration (pos? (.toMillis duration)))
    duration))

(defn- run-with-timeout [f ^Duration timeout]
  (let [timeout (normalize-timeout timeout)
        timeout-ms (some-> timeout .toMillis)]
    (if (and timeout-ms (pos? timeout-ms))
      (let [fut (future (f))
            result (deref fut timeout-ms timeout-sentinel)]
        (if (= timeout-sentinel result)
          (do
            (future-cancel fut)
            (log/warn "Benchmark exceeded timeout, cancelling" {:timeout timeout})
            (throw (ex-info (format "Benchmark exceeded timeout (%s)" timeout)
                            {:xtdb.bench/timeout timeout})))
          result))
      (f))))

(defn wrap-in-catch [f]
  (fn [& args]
    (try
      (apply f args)
      (catch OutOfMemoryError oom
        (log/error oom "OutOfMemoryError - forcing JVM exit")
        (System/exit 1))
      (catch Throwable t
        (log/error t (str "Error while executing " f))
        (throw t)))))

(defrecord Worker [node random clock bench-id jvm-id bench-log-wrt])

(defn current-timestamp ^java.time.Instant [worker]
  (.instant ^Clock (:clock worker)))

(defn rng ^java.util.Random [worker] (:random worker))

(defn current-timestamp-ms ^long [worker] (.millis ^Clock (:clock worker)))

(defn sample-gaussian ^long [worker ^AtomicLong !counter]
  (let [v (.get !counter)]
    (-> (long (.nextGaussian (rng worker) (* v 0.5) (/ v 6.0)))
        (max 0)
        (min (dec v)))))

(defn sample-flat ^long [worker, ^AtomicLong !counter]
  (let [v (.get !counter)]
    (when (pos? v)
      (.nextLong (rng worker) v))))

(defn inc-count! [^AtomicLong !counter]
  (.getAndIncrement !counter))

(defn set-count! [^AtomicLong !counter, ^long v]
  (.set !counter (inc v)))

(defn weighted-sample-fn
  "Aliased random sampler:

  https://www.peterstefek.me/alias-method.html

  Given a seq of [item weight] pairs, return a function who when given a Random will return an item according to the weight distribution."
  [weighted-items]
  (case (count weighted-items)
    0 (constantly nil)
    1 (constantly (ffirst weighted-items))

    (let [total (reduce + (map second weighted-items))
          normalized-items (mapv (fn [[item weight]] [item (double (/ weight total))]) weighted-items)
          len (count normalized-items)
          pq (doto (.create (MinMaxPriorityQueue/orderedBy ^Comparator (fn [[_ w] [_ w2]] (compare w w2))))
               (.addAll normalized-items))
          avg (/ 1.0 len)
          parts (object-array len)
          epsilon 0.00001]

      (dotimes [i len]
        (let [[smallest small-weight] (.pollFirst pq)
              overfill (- avg small-weight)]
          (if (< epsilon overfill)
            (let [[largest large-weight] (.pollLast pq)
                  new-weight (- large-weight overfill)]
              (when (< epsilon new-weight)
                (.add pq [largest new-weight]))
              (aset parts i [small-weight smallest largest]))
            (aset parts i [small-weight smallest smallest]))))

      ^{:table parts}
      (fn sample-weighting [^Random random]
        (let [i (.nextInt random len)
              [split small large] (aget parts i)]
          (if (<= (/ (.nextDouble random) len) (double split)) small large))))))

(defn random-seq [worker opts f & args]
  (let [{:keys [min, max, unique]} opts]
    (->> (repeatedly #(apply f worker args))
         (take (+ min (.nextInt (rng worker) (- max min))))
         ((if unique distinct identity)))))

(defn random-str
  ([worker] (random-str worker 1 100))
  ([worker min-len max-len]
   (let [random (rng worker)
         len (max 0 (+ min-len (.nextInt random max-len)))
         buf (byte-array (* 2 len))
         _ (.nextBytes random buf)]
     (.toString (BigInteger. 1 buf) 16))))

(defn random-nth [worker coll]
  (when (seq coll)
    (let [idx (.nextInt (rng worker) (count coll))]
      (nth coll idx nil))))

(defn random-bool [worker]
  (.nextBoolean (rng worker)))

(def kb 1024)
(def mb (* kb 1024))
(def gb (* mb 1024))

(defn get-system-info []
  (let [si (SystemInfo.)
        os (.getOperatingSystem si)
        os-version (.getVersionInfo os)
        hardware (.getHardware si)
        cpu (.getProcessor hardware)]
    {:jre (System/getProperty "java.vendor.version")
     :java-opts (str/join " " (.getInputArguments (ManagementFactory/getRuntimeMXBean)))
     :max-heap (format "%sMB" (quot (.maxMemory (Runtime/getRuntime)) mb))
     :arch (System/getProperty "os.arch")
     :os (->> [(.getFamily os) (.getCodeName os-version) (.getVersion os-version)]
              (remove str/blank?)
              (str/join " "))
     :memory (format "%sGB" (quot (.getTotal (.getMemory hardware)) gb))
     :cpu (format "%s, %s cores, %.2fGHZ max"
                  (.getName (.getProcessorIdentifier cpu))
                  (.getPhysicalProcessorCount cpu)
                  (double (/ (.getMaxFreq cpu) 1e9)))}))

(defn log-report [{:keys [bench-id jvm-id ^Writer bench-log-wrt] :as _worker} report]
  (let [log-record (json/write-str (assoc report :bench-id bench-id :jvm-id jvm-id))]
    (println log-record)
    (when bench-log-wrt
      (binding [*out* bench-log-wrt]
        (println log-record)
        (.flush bench-log-wrt)))))

(def ^:dynamic *registry* nil)

(def percentiles [0.75 0.85 0.95 0.98 0.99 0.999])

(defn- get-transaction-metrics
  "Extract transaction metrics from the registry. Returns a map of transaction name to metrics.
   Filters out internal metrics (those with dots in the name like jvm.gc.pause, query.timer, etc.)"
  []
  (when *registry*
    (->> (.getMeters *registry*)
         (filter #(instance? Timer %))
         (keep (fn [^Timer timer]
                 (let [id (.getId timer)
                       name (.getName id)]
                   ;; Only include benchmark transactions (no dots in name)
                   ;; This excludes jvm.gc.pause, query.timer, compactor.job.timer, tx.op.timer, etc.
                   (when-not (str/includes? name ".")
                     [name {:count (.count timer)
                            :total-time-ms (/ (.totalTime timer TimeUnit/MILLISECONDS) 1.0)
                            :mean-ms (/ (.mean timer TimeUnit/MILLISECONDS) 1.0)
                            :max-ms (/ (.max timer TimeUnit/MILLISECONDS) 1.0)}]))))
         (into {}))))

(defn wrap-stage [f {:keys [stage setup?]}]
  (fn instrumented-stage [worker]
    (log/info "Starting stage:" stage (when setup? "(setup)"))
    (let [start-ms (System/currentTimeMillis)]
      (f worker)
      (let [elapsed-ms (- (System/currentTimeMillis) start-ms)]
        (when setup?
          (swap! (:setup-time-ms worker) + elapsed-ms))
        (log-report worker {:stage stage
                            :time-taken-ms elapsed-ms
                            :setup setup?}))
      (log/info "Done stage:" stage))))

(defn wrap-transaction [f {:keys [transaction labels]}]
  (let [timer-delay (delay
                      (when *registry*
                        (let [timer (Timer/builder (name transaction))]
                          (doseq [[^String k ^String v] labels]
                            (.tag timer k v))
                          (-> timer
                              (.publishPercentiles (double-array percentiles))
                              (.maximumExpectedValue (Duration/ofHours 8))
                              (.minimumExpectedValue (Duration/ofNanos 1))
                              (.register *registry*)))))]
    (fn instrumented-transaction [worker]
      (if-some [^Timer timer @timer-delay]
        (.recordCallable timer ^Callable (fn [] (f worker)))
        (f worker)))))

(defn wrap-task [f task]
  (let [{:keys [stage, transaction]} task]
    (cond
      stage (wrap-stage f task)
      transaction (wrap-transaction f task)
      :else f)))

(defn- lift-f [f]
  (if (vector? f) #(apply (first f) % (rest f)) f))

(defn- compile-task [{:keys [t], :as task}]
  (-> (case t
        nil (constantly nil)

        :do (let [{:keys [tasks]} task
                  fns (mapv compile-task tasks)]
              (fn run-do [worker]
                (doseq [f fns]
                  (f worker))))

        :call (let [{:keys [f]} task]
                (lift-f f))

        :pool (let [{:keys [^Duration duration ^Duration think ^Duration join-wait thread-count pooled-task]} task
                    think-ms (.toMillis (or think Duration/ZERO))
                    f (compile-task pooled-task)

                    executor (Executors/newFixedThreadPool thread-count (util/->prefix-thread-factory "xtdb-benchmark"))

                    thread-loop (fn run-pool-thread-loop [worker]
                                  (loop [wait-until (+ (current-timestamp-ms worker) (.toMillis duration))]
                                    (f worker)
                                    (when (< (current-timestamp-ms worker) wait-until)
                                      ;; Sleep only the remaining time (or think time, whichever is less)
                                      (let [remaining-ms (- wait-until (current-timestamp-ms worker))
                                            sleep-ms (min think-ms remaining-ms)]
                                        (when (pos? sleep-ms)
                                          (Thread/sleep sleep-ms))
                                        (recur wait-until)))))

                    start-thread (fn [root-worker _i]
                                   (let [bindings (get-thread-bindings)
                                         worker (assoc root-worker :random (Random. (.nextLong (rng root-worker))))]
                                     (.submit executor ^Runnable (fn []
                                                                   (push-thread-bindings bindings)
                                                                   (-> worker
                                                                       (assoc :thread-name (.getName (Thread/currentThread)))
                                                                       thread-loop)))))]

                (fn run-pool [worker]
                  (let [futures (mapv #(start-thread worker %) (range thread-count))]
                    (Thread/sleep (.toMillis duration))
                    (.shutdown executor)
                    (when-not (.awaitTermination executor (.toMillis join-wait) TimeUnit/MILLISECONDS)
                      (log/warn "Pool threads did not stop within join-wait, forcing interruption")
                      (.shutdownNow executor)
                      (when-not (.awaitTermination executor 30 TimeUnit/SECONDS)
                        (log/warn "Pool threads still running after forced shutdown - will stop when node closes")))
                    (doseq [f futures]
                      (try
                        (deref f 5 TimeUnit/SECONDS) ; will throw ExecutionException if the Runnable raised
                        (catch java.util.concurrent.TimeoutException _
                          (log/warn "Worker thread did not complete in time, skipping"))
                        (catch java.util.concurrent.ExecutionException e
                          (let [cause (.getCause e)]
                            (when (instance? OutOfMemoryError cause)
                              (log/error cause "OutOfMemoryError in worker thread - forcing JVM exit")
                              (System/exit 1))
                            ;; Log but don't rethrow - connection errors during shutdown are expected
                            (if (or (instance? java.sql.SQLException cause)
                                    (re-find #"connection.*closed" (str (.getMessage cause))))
                              (log/debug cause "Expected connection error during shutdown")
                              (log/error cause "Benchmark worker failed in :pool" {:task task})))))))))

        :concurrently (let [{:keys [^Duration duration, ^Duration join-wait, thread-tasks]} task
                            thread-task-fns (mapv compile-task thread-tasks)

                            executor (Executors/newFixedThreadPool (count thread-tasks) (util/->prefix-thread-factory "xtdb-benchmark"))

                            start-thread (fn [root-worker _i f]
                                           (let [bindings (get-thread-bindings)
                                                 worker (assoc root-worker :random (Random. (.nextLong (rng root-worker))))]
                                             (.submit executor ^Runnable (fn [] (push-thread-bindings bindings)
                                                                           (-> worker
                                                                               (assoc :thread-name (.getName (Thread/currentThread)))
                                                                               f)))))]
                        (fn run-concurrently [worker]
                          (let [futures (mapv #(start-thread worker %1 %2) (range (count thread-task-fns)) thread-task-fns)]
                            (Thread/sleep (.toMillis duration))
                            (.shutdown executor)
                            (when-not (.awaitTermination executor (.toMillis join-wait) TimeUnit/MILLISECONDS)
                              (log/warn "Threads did not stop within join-wait, forcing interruption")
                              (.shutdownNow executor)
                              (when-not (.awaitTermination executor 30 TimeUnit/SECONDS)
                                (log/warn "Threads still running after forced shutdown - will stop when node closes")))
                            (doseq [f futures]
                              (try
                                (deref f 5 TimeUnit/SECONDS) ; will throw ExecutionException if the Runnable raised
                                (catch java.util.concurrent.TimeoutException _
                                  (log/warn "Worker thread did not complete in time, skipping"))
                                (catch java.util.concurrent.ExecutionException e
                                  (let [cause (.getCause e)]
                                    (when (instance? OutOfMemoryError cause)
                                      (log/error cause "OutOfMemoryError in worker thread - forcing JVM exit")
                                      (System/exit 1))
                                    ;; Log but don't rethrow - connection errors during shutdown are expected
                                    (if (or (instance? java.sql.SQLException cause)
                                            (re-find #"connection.*closed" (str (.getMessage cause))))
                                      (log/debug cause "Expected connection error during shutdown")
                                      (log/error cause "Benchmark worker failed in :concurrently" {:task task})))))))))

        :pick-weighted (let [{:keys [choices]} task
                             sample-fn (weighted-sample-fn (mapv (fn [[weight task]] [(compile-task task) weight]) choices))]
                         (if (empty? choices)
                           (constantly nil)
                           (fn run-pick-weighted [worker]
                             (let [f (sample-fn (rng worker))]
                               (f worker)))))

        :freq-job (let [{:keys [^Duration duration,
                                ^Duration freq,
                                job-task]} task
                        f (compile-task job-task)
                        duration-ms (.toMillis (or duration Duration/ZERO))
                        freq-ms (.toMillis (or freq Duration/ZERO))]
                    (fn run-freq-job [worker]
                      (loop [wait-until (+ (current-timestamp-ms worker) duration-ms)]
                        (f worker)
                        (when (< (current-timestamp-ms worker) wait-until)
                          ;; Sleep only the remaining time (or freq time, whichever is less)
                          (let [remaining-ms (- wait-until (current-timestamp-ms worker))
                                sleep-ms (min freq-ms remaining-ms)]
                            (when (pos? sleep-ms)
                              (Thread/sleep sleep-ms))
                            (recur wait-until)))))))

      (wrap-task task)))

(defn- get-env [k]
  (let [v (System/getenv k)]
    (when-not (str/blank? v) v)))

(defn- get-run-context []
  (->> {"GIT_SHA" :git-sha
        "GIT_BRANCH" :git-branch
        "GITHUB_REPOSITORY" :github-repo
        "GITHUB_RUN_ID" :github-run-id}
       (keep (fn [[env-key map-key]]
               (when-let [v (get-env env-key)]
                 [map-key v])))
       (into {})))

(defn compile-benchmark [{:keys [bench-log-file title benchmark-type seed ->state parameters timeout], :or {seed 0}, :as benchmark}]
  (let [fns (mapv compile-task (:tasks benchmark))]
    (fn run-benchmark [node]
      (letfn [(execute []
                (util/with-open [bench-log-wrt (when bench-log-file (io/writer bench-log-file))]
                  (let [worker (into (assoc (->Worker node (Random. seed) (Clock/systemUTC) (random-uuid) (System/getProperty "user.name") bench-log-wrt)
                                            :setup-time-ms (atom 0))
                                     (cond
                                       (vector? ->state) (apply (first ->state) (rest ->state))
                                       (fn? ->state) (->state)))
                        start-ms (System/currentTimeMillis)
                        run-context (get-run-context)
                        system-info (get-system-info)]
                    (log-report worker (merge {:benchmark title
                                               :benchmark-type benchmark-type
                                               :stage "init"
                                               :parameters parameters
                                               :system system-info}
                                              run-context
                                              (when timeout {:timeout timeout})))

                    (doseq [f fns]
                      (f worker))

                    (let [total-ms (- (System/currentTimeMillis) start-ms)
                          setup-ms @(:setup-time-ms worker)
                          benchmark-ms (- total-ms setup-ms)
                          tx-metrics (get-transaction-metrics)
                          tx-count (reduce + 0 (map :count (vals tx-metrics)))
                          ;; Calculate throughput using configured duration if available
                          duration-secs (when-let [^Duration d (:duration parameters)]
                                          (/ (.toMillis d) 1000.0))
                          throughput (when (and duration-secs (pos? duration-secs))
                                       (/ tx-count duration-secs))]
                      (log-report worker (cond-> {:benchmark title
                                                  :benchmark-type benchmark-type
                                                  :stage "summary"
                                                  :parameters parameters
                                                  :system system-info
                                                  :time-taken-ms benchmark-ms}
                                           (pos? setup-ms) (assoc :setup-time-ms setup-ms
                                                                  :total-time-ms total-ms)
                                           (pos? tx-count) (assoc :transaction-count tx-count
                                                                  :transactions tx-metrics)
                                           throughput (assoc :throughput throughput)
                                           timeout (assoc :timeout timeout)
                                           (seq run-context) (merge run-context)))
                      ;; Human-readable summary
                      (let [secs (/ benchmark-ms 1000.0)
                            mins (int (/ secs 60))
                            remaining-secs (- secs (* mins 60))]
                        (if (>= mins 1)
                          (log/infof "Benchmark Time: %dm %.1fs%s"
                                     mins remaining-secs
                                     (if (pos? setup-ms) (format " (excludes %.1fs setup)" (/ setup-ms 1000.0)) ""))
                          (log/infof "Benchmark Time: %.1fs%s"
                                     secs
                                     (if (pos? setup-ms) (format " (excludes %.1fs setup)" (/ setup-ms 1000.0)) "")))
                        (when (pos? tx-count)
                          (let [sorted-txs (->> tx-metrics (sort-by (comp :count val) >))
                                name-width (max 12 (apply max (map (comp count key) sorted-txs)))
                                fmt-str (str "  %-" name-width "s  %6d txns  avg %8.1fms  max %8.1fms  total %8.1fs")]
                            (log/infof "Transaction Stats (%d total%s):"
                                       tx-count
                                       (if throughput (format ", %.1f tx/sec" throughput) ""))
                            (doseq [[tx-name {:keys [count total-time-ms mean-ms max-ms]}] sorted-txs]
                              (log/infof fmt-str tx-name count mean-ms max-ms (/ total-time-ms 1000.0))))))))))]
        (run-with-timeout execute timeout)))))

(defn sync-node
  ([node] (xt-log/sync-node node))

  ([node ^Duration timeout]
   (xt-log/sync-node node timeout)))

(defn flush-block! [node]
  (tu/flush-block! node))

(defn compact! [node]
  (c/compact-all! node (Duration/ofMinutes 10)))

(defn generate
  ([worker table f n]
   (let [doc-seq (remove nil? (repeatedly (long n) (partial f worker)))
         partition-count 512]
     (doseq [batch (partition-all partition-count doc-seq)]
       (xt/submit-tx (:node worker) [(into [:put-docs table] batch)])))))

(defmulti cli-flags identity
  :default ::default)

(defmethod cli-flags ::default [_] [])

(defmulti ->benchmark
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [benchmark-type opts]
    benchmark-type)
  :default ::default)

(defn run-benchmark [benchmark {:keys [node-dir no-load? config-file bench-log-file timeout tracing-endpoint] :as _opts}]
  (let [benchmark-fn (compile-benchmark (-> benchmark
                                            (assoc :bench-log-file bench-log-file)
                                            (assoc :timeout timeout)))]
    (if config-file
      (do
        (log/info "Running node from config file:" config-file)
        (try
          (with-open [node (xtn/start-node config-file)]
            (binding [tu/*allocator* (util/component node :xtdb/allocator)
                      *registry* (util/component node :xtdb.metrics/registry)]
              (benchmark-fn node)))
          (catch Throwable t
            (if @shutting-down?
              (log/warn t "Error during shutdown (ignored):")
              (throw t)))))
      (letfn [(run [node-dir]
                (try
                  (with-open [node (tu/->local-node (cond-> {:node-dir node-dir
                                                             :instant-src (InstantSource/system)}
                                                      tracing-endpoint (assoc :tracer {:enabled? true
                                                                                       :endpoint tracing-endpoint
                                                                                       :service-name "xtdb-bench"})))]
                    (binding [tu/*allocator* (util/component node :xtdb/allocator)
                              *registry* (util/component node :xtdb.metrics/registry)]
                      (benchmark-fn node)))
                  (catch Throwable t
                    (if @shutting-down?
                      (log/warn t "Error during shutdown (ignored):")
                      (throw t)))))]
        (if node-dir
          (do
            (log/info "Using node dir:" (str node-dir))
            (when-not no-load?
              (util/delete-dir node-dir))
            (run node-dir))

          (util/with-tmp-dirs #{node-tmp-dir}
            (log/info "Using temporary dir: " node-tmp-dir)
            (run node-tmp-dir)))))))

(defn if-it-exists [^File f]
  (when (.exists f)
    f))

(defn- parse-duration-arg [s]
  (Duration/parse s))

(def ^:private default-cli-flags
  [[nil "--node-dir NODE_DIR"
    "Directory to run the node in - will clear before running the benchmark unless `--no-load` is provided."
    :parse-fn util/->path]

   [nil "--no-load" "don't run any load phases (use with an existing directory)"
    :id :no-load?]

   ["-f" "--config-file CONFIG_FILE" "Config file to load XTDB options from - EDN, YAML"
    :id :config-file
    :parse-fn io/file
    :validate [if-it-exists "Config file doesn't exist"
               #(contains? #{"yaml"} (util/file-extension %)) "Config file must be .yaml"]]
   [nil "--bench-log-file BENCH_LOG_FILE" "Log file that saves the benchmark results in JSON format"
    :id :bench-log-file
    :parse-fn io/file]

   [nil "--timeout DURATION"
    "Maximum wall-clock runtime before aborting the benchmark (ISO-8601). If omitted, no timeout is applied."
    :id :timeout
    :parse-fn parse-duration-arg
    :validate [#(not (neg? (.toMillis ^Duration %))) "Timeout must be >= PT0S"]]

   [nil "--tracing-endpoint ENDPOINT"
    "OTLP HTTP endpoint for sending traces (e.g., http://localhost:4318/v1/traces). Only used when no config file is provided."
    :id :tracing-endpoint]])

(def ^:private default-github-action-url
  "https://api.github.com/repos/xtdb/xtdb/actions/workflows/nightly-benchmark-cleanup.yml/dispatches")

(defn trigger-cleanup [benchmark-type success?]
  (when-let [pat (System/getenv "GITHUB_PAT")]
    (try
      (let [node-id (System/getenv "XTDB_NODE_ID")
            git-branch (or (get-env "GIT_BRANCH") "main")
            run-queue (System/getenv "RUN_QUEUE")
            workflow-url (or (get-env "GITHUB_ACTION_URL") default-github-action-url)]
        (http/post workflow-url
                   {:headers {"Accept" "application/vnd.github+json"
                              "Authorization" (str "Bearer " pat)}
                    :content-type :json
                    :body (json/write-str {"ref" git-branch
                                           "inputs" (cond-> {"benchType" (name benchmark-type)
                                                             "status" (if success? "success" "failure")
                                                             "nodeId" node-id}
                                                      run-queue (assoc "runQueue" "true"))})})
        (log/info "Triggered cleanup workflow for" benchmark-type "on" git-branch))
      (catch Exception e
        (log/warn e "Failed to trigger cleanup workflow")))))

(defn -main [benchmark-type & args]
  (util/install-uncaught-exception-handler!)
  (logging/set-from-env! (System/getenv))

  (require (case benchmark-type
             "tsbs-iot" 'xtdb.bench.tsbs
             "ingestTxOverhead" 'xtdb.bench.ingest-tx-overhead
             (symbol (str "xtdb.bench." benchmark-type))))

  (let [benchmark-type (case benchmark-type
                         "ingestTxOverhead" :ingest-tx-overhead
                         (keyword benchmark-type))
        {:keys [options errors summary]} (cli/parse-opts args (concat (cli-flags benchmark-type)
                                                                      default-cli-flags))]
    (cond
      (seq errors) (binding [*out* *err*]
                     (doseq [error errors]
                       (println error))
                     (System/exit 2))

      (:help options) (binding [*out* *err*]
                        (println summary)
                        (System/exit 0))

      :else (let [main-thread (Thread/currentThread)]
              (-> (Runtime/getRuntime)
                  (.addShutdownHook (Thread. (fn []
                                               (log/info "Received shutdown signal, initiating graceful shutdown...")
                                               (reset! shutting-down? true)
                                               (let [shutdown-ms 10000]
                                                 (.interrupt main-thread)
                                                 (.join main-thread shutdown-ms)
                                                 (if (.isAlive main-thread)
                                                   (do
                                                     (log/warn "could not stop benchmark cleanly after" shutdown-ms "ms, forcing exit")
                                                     (-> (Runtime/getRuntime) (.halt 1)))
                                                   (log/info "Benchmark stopped."))))
                                             "xtdb.shutdown-hook-thread")))

              (let [ok? (atom false)]
                (try
                  (run-benchmark (->benchmark benchmark-type options) options)
                  (reset! ok? true)
                  (catch Throwable t
                    (if @shutting-down?
                      (log/warn t "Error during shutdown (ignored):")
                      (do
                        (when (instance? InterruptedException t)
                          (.interrupt (Thread/currentThread)))
                        (log/error t "Benchmark failed:")
                        (throw t))))
                  (finally
                    ;; Mark as shutting down for cleanup phase
                    (reset! shutting-down? true)
                    (try
                      (trigger-cleanup benchmark-type @ok?)
                      ;; Signal to dump-uploader sidecar that cleanup was triggered
                      (spit "/dumps/.cleanup-triggered" "true")
                      (catch Throwable u
                        (log/warn u "Cleanup trigger failed (ignored):")))))))))

  (shutdown-agents))
