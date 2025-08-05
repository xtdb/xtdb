(ns xtdb.bench
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.logging :as logging]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (com.google.common.collect MinMaxPriorityQueue)
           (io.micrometer.core.instrument Timer)
           (java.io File Writer)
           (java.lang.management ManagementFactory)
           (java.time Clock Duration InstantSource)
           java.time.Duration
           (java.util Comparator Random)
           (java.util.concurrent Executors TimeUnit)
           (java.util.concurrent.atomic AtomicLong)
           (oshi SystemInfo)))

(defn wrap-in-catch [f]
  (fn [& args]
    (try
      (apply f args)
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

(defn wrap-stage [f {:keys [stage]}]
  (fn instrumented-stage [worker]
    (log/info "Starting stage:" stage)
    (let [start-ms (System/currentTimeMillis)]
      (f worker)
      (log-report worker {:stage stage,
                          :time-taken-ms (- (System/currentTimeMillis) start-ms)})
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
                    sleep (if (pos? think-ms) #(Thread/sleep think-ms) (constantly nil))
                    f (compile-task pooled-task)

                    executor (Executors/newFixedThreadPool thread-count (util/->prefix-thread-factory "xtdb-benchmark"))

                    thread-loop (fn run-pool-thread-loop [worker]
                                  (loop [wait-until (+ (current-timestamp-ms worker) (.toMillis duration))]
                                    (f worker)
                                    (when (< (current-timestamp-ms worker) wait-until)
                                      (sleep)
                                      (recur wait-until))))

                    start-thread (fn [root-worker _i]
                                   (let [bindings (get-thread-bindings)
                                         worker (assoc root-worker :random (Random. (.nextLong (rng root-worker))))]
                                     (.submit executor ^Runnable (fn []
                                                                   (push-thread-bindings bindings)
                                                                   (-> worker
                                                                       (assoc :thread-name (.getName (Thread/currentThread)))
                                                                       thread-loop)))))]

                (fn run-pool [worker]
                  (run! #(start-thread worker %) (range thread-count))
                  (Thread/sleep (.toMillis duration))
                  (.shutdown executor)
                  (when-not (.awaitTermination executor (.toMillis join-wait) TimeUnit/MILLISECONDS)
                    (.shutdownNow executor)
                    (when-not (.awaitTermination executor (.toMillis join-wait) TimeUnit/MILLISECONDS)
                      (throw (ex-info "Pool threads did not stop within join-wait" {:task task, :executor executor}))))))

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
                          (dorun (map-indexed #(start-thread worker %1 %2) thread-task-fns))
                          (Thread/sleep (.toMillis duration))
                          (.shutdown executor)
                          (when-not (.awaitTermination executor (.toMillis join-wait) TimeUnit/MILLISECONDS)
                            (.shutdownNow executor)
                            (when-not (.awaitTermination executor (.toMillis join-wait) TimeUnit/MILLISECONDS)
                              (throw (ex-info "Task threads did not stop within join-wait" {:task task, :executor executor}))))))

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
                        freq-ms (.toMillis (or freq Duration/ZERO))
                        sleep (if (pos? freq-ms) #(Thread/sleep freq-ms) (constantly nil))]
                    (fn run-freq-job [worker]
                      (loop [wait-until (+ (current-timestamp-ms worker) duration-ms)]
                        (f worker)
                        (when (< (current-timestamp-ms worker) wait-until)
                          (sleep)
                          (recur wait-until))))))

      (wrap-task task)))

(defn compile-benchmark [{:keys [bench-log-file title seed ->state], :or {seed 0}, :as benchmark}]
  (let [fns (mapv compile-task (:tasks benchmark))]
    (fn run-benchmark [node]
      (util/with-open [bench-log-wrt (when bench-log-file (io/writer bench-log-file))]
        (let [worker (into (->Worker node (Random. seed) (Clock/systemUTC) (random-uuid) (System/getProperty "user.name") bench-log-wrt)
                           (cond
                             (vector? ->state) (apply (first ->state) (rest ->state))
                             (fn? ->state) (->state)))
              start-ms (System/currentTimeMillis)]
          (doseq [f fns]
            (f worker))

          (log-report worker {:benchmark title
                              :system (get-system-info)
                              :time-taken-ms (- (System/currentTimeMillis) start-ms)}))))))

(defn sync-node
  ([node] (xt-log/sync-node node))

  ([node ^Duration timeout]
   (xt-log/sync-node node timeout)))

(defn finish-block! [node]
  (tu/finish-block! node))

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

(defn run-benchmark [benchmark {:keys [node-dir no-load? config-file bench-log-file]}]
  (let [benchmark-fn (compile-benchmark (-> benchmark
                                            (assoc :bench-log-file bench-log-file)))]
    (if config-file
      (do
        (log/info "Running node from config file:" config-file)
        (with-open [node (xtn/start-node config-file)]
          (benchmark-fn node)))
      (letfn [(run [node-dir]
                (with-open [node (tu/->local-node {:node-dir node-dir
                                                   :instant-src (InstantSource/system)})]
                  (binding [tu/*allocator* (util/component node :xtdb/allocator)
                            *registry* (util/component node :xtdb.metrics/registry)]
                    (benchmark-fn node))))]
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
    :parse-fn io/file]])

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
                                               (let [shutdown-ms 10000]
                                                 (.interrupt main-thread)
                                                 (.join main-thread shutdown-ms)
                                                 (if (.isAlive main-thread)
                                                   (do
                                                     (log/warn "could not stop benchmark cleanly after" shutdown-ms "ms, forcing exit")
                                                     (-> (Runtime/getRuntime) (.halt 1)))

                                                   (log/info "Benchmark stopped."))))

                                             "xtdb.shutdown-hook-thread")))

              (run-benchmark (->benchmark benchmark-type options) options))))

  (shutdown-agents))
