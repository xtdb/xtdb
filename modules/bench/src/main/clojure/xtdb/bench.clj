(ns xtdb.bench
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [xtdb.util :as util])
  (:import (com.google.common.collect MinMaxPriorityQueue)
           (java.lang.management ManagementFactory)
           (java.time Instant Duration Clock)
           java.time.Duration
           (java.util Random Comparator)
           (java.util.concurrent ConcurrentHashMap Executors TimeUnit)
           (java.util.concurrent.atomic AtomicLong)
           (java.util.function Function)
           (oshi SystemInfo)))

(defn wrap-in-catch [f]
  (fn [& args]
    (try
      (apply f args)
      (catch Throwable t
        (log/error t (str "Error while executing " f))
        (throw t)))))

(defrecord Worker [sut random domain-state custom-state clock bench-id jvm-id])

(defn current-timestamp ^Instant [worker]
  (.instant ^Clock (:clock worker)))

(defn counter ^AtomicLong [worker domain]
  (.computeIfAbsent ^ConcurrentHashMap (:domain-state worker) domain (reify Function (apply [_ _] (AtomicLong.)))))

(defn rng ^Random [worker] (:random worker))

(defn current-timestamp-ms ^long [worker] (.millis ^Clock (:clock worker)))

(defn id
  "Returns an identity fn. Would be _the identity_ but with uh... identity, e.g
   the function returned is a new java object on each call."
  []
  (fn [n] n))

(defn increment [worker domain] (domain (.getAndIncrement (counter worker domain))))

(defn set-domain [worker domain cnt] (.getAndAdd (counter worker domain) cnt))

(defn- nat-or-nil [n] (when (nat-int? n) n))

(defn sample-gaussian [worker domain]
  (let [random (rng worker)
        long-counter (counter worker domain)]
    ;; not a real gaussian, we cut of some bits at the tails
    (some-> (min (dec (.get long-counter)) (max 0 (Math/round (* (.get long-counter) (* 0.5 (+ 1.0 (.nextGaussian random)))))))
            long
            nat-or-nil
            domain)))

(defn sample-flat [worker domain]
  (let [random (rng worker)
        long-counter (counter worker domain)]
    (some-> (min (dec (.get long-counter)) (Math/round (* (.get long-counter) (.nextDouble random))))
            long
            nat-or-nil
            domain)))

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
          pq (doto (.create (MinMaxPriorityQueue/orderedBy ^Comparator (fn [[_ w] [_ w2]] (compare w w2)))) (.addAll normalized-items))
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

(defn get-system-info
  "Returns data about the JVM, hardware / OS running this JVM."
  []
  (let [si (SystemInfo.)
        os (.getOperatingSystem si)
        os-version (.getVersionInfo os)
        os-codename (.getCodeName os-version)
        os-version-number (.getVersion os-version)
        arch (System/getProperty "os.arch")
        hardware (.getHardware si)
        cpu (.getProcessor hardware)
        cpu-identifier (.getProcessorIdentifier cpu)
        cpu-name (.getName cpu-identifier)
        cpu-core-count (.getPhysicalProcessorCount cpu)
        cpu-max-freq (.getMaxFreq cpu)
        ram (.getMemory hardware)
        kb (* 1024)
        mb (* kb 1024)
        gb (* mb 1024)
        runtime-mx-bean (ManagementFactory/getRuntimeMXBean)
        args (.getInputArguments runtime-mx-bean)]
    {:jre (System/getProperty "java.vendor.version")
     :java-opts (str/join " " args)
     :max-heap (format "%sMB" (quot (.maxMemory (Runtime/getRuntime)) mb))
     :arch arch
     :os (str/join " " (remove str/blank? [(.getFamily os) os-codename os-version-number]))
     :memory (format "%sGB" (quot (.getTotal ram) gb))
     :cpu (format "%s, %s cores, %.2fGHZ max" cpu-name cpu-core-count (double (/ cpu-max-freq 1e9)))}))

(comment
  ;; possible task types
  {:t :do
   :tasks [....]}

  {:t :call
   :f '(fn [worker]) ;; or [fn & remaining-args]
   }

  {:t :pool
   :duration (Duration/ZERO)
   :think (Duration/ZERO)
   :join-wait (Duration/ZERO)
   :thread-count 1
   :pooled-task 'some-task}

  {:t :concurrently
   :duration 'duration
   :join-wait 'time-to-wait-for-joining
   :thread-tasks '[task1 task2]}

  {:t :pick-weighted
   :choices [['task-1 12.0]
             ['task-2 1.0]]}

  {:t :freq-job
   :duration 'how-long-this-freq-job-should-be-executed
   :freq 'sleep-time-before-next-run
   :job-task 'task}
  )

(defn log-report [{:keys [bench-id jvm-id] :as _worker} report]
  (println (json/write-str (assoc report :bench-id bench-id :jvm-id jvm-id))))

(defn compile-benchmark [{:keys [title] :as benchmark} hook]
  (let [seed (:seed benchmark 0)
        lift-f (fn [f] (if (vector? f) #(apply (first f) % (rest f)) f))
        compile-task
        (fn compile-task [{:keys [t] :as task}]
          (hook
           task
           (case t
             nil (constantly nil)

             :do
             (let [{:keys [tasks]} task
                   fns (mapv compile-task tasks)]
               (fn run-do [worker]
                 (doseq [f fns]
                   (f worker))))

             :call
             (let [{:keys [f]} task]
               (lift-f f))

             :pool
             (let [{:keys [^Duration duration
                           ^Duration think
                           ^Duration join-wait
                           thread-count
                           pooled-task]} task

                   think-ms (.toMillis (or think Duration/ZERO))
                   sleep (if (pos? think-ms) #(Thread/sleep think-ms) (constantly nil))
                   f (compile-task pooled-task)

                   executor
                   (Executors/newFixedThreadPool thread-count (util/->prefix-thread-factory "core2-benchmark"))

                   thread-loop
                   (fn run-pool-thread-loop [worker]
                     (loop [wait-until (+ (current-timestamp-ms worker) (.toMillis duration))]
                       (f worker)
                       (when (< (current-timestamp-ms worker) wait-until)
                         (sleep)
                         (recur wait-until))))

                   start-thread
                   (fn [root-worker _i]
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

             :concurrently
             (let [{:keys [^Duration duration,
                           ^Duration join-wait,
                           thread-tasks]} task

                   thread-task-fns (mapv compile-task thread-tasks)

                   executor (Executors/newFixedThreadPool (count thread-tasks) (util/->prefix-thread-factory "core2-benchmark"))

                   start-thread
                   (fn [root-worker _i f]
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

             :pick-weighted
             (let [{:keys [choices]} task
                   sample-fn (weighted-sample-fn (mapv (fn [[task weight]] [(compile-task task) weight]) choices))]
               (if (empty? choices)
                 (constantly nil)
                 (fn run-pick-weighted [worker]
                   (let [f (sample-fn (rng worker))]
                     (f worker)))))

             :freq-job
             (let [{:keys [^Duration duration,
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
                     (recur wait-until))))))))
        fns (mapv compile-task (:tasks benchmark))]

    (fn run-benchmark [sut]
      (let [clock (Clock/systemUTC)
            domain-state (ConcurrentHashMap.)
            custom-state (ConcurrentHashMap.)
            root-random (Random. seed)
            worker (->Worker sut root-random domain-state custom-state clock (random-uuid) (System/getProperty "user.name"))
            start-ms (System/currentTimeMillis)]
        (doseq [f fns]
          (f worker))

        (log-report worker {:benchmark title
                            :system (get-system-info)
                            :time-taken-ms (- (System/currentTimeMillis) start-ms)})))))

(comment
  ;; low level benchmark evaluation in process...
  ;; a benchmark is a data structure which is compiled to produce a function of system-under-test to report.
  ;; compiler takes a hook fn for applying measurement policy independently of the benchmark definition.

  ;; e.g compiling a benchmark that targets an xt node will produce a function that takes a node instance as its parameter.

  (def foo-bench
    (compile-benchmark
     ;; benchmark definition
     {:seed 0
      :tasks [{:t :call,
               :stage :foo
               ;; receives system-under test under :sut, this arg will be threaded through when the benchmark
               ;; is eval'd, e.g an xt node.
               :f (fn [_worker] (Thread/sleep 100))}]}
     ;; middleware hook for injecting measurement, proxies and what not
     (fn [_task f] f)))

  ;; provide sut here, using 42 because the runner does not care
  (foo-bench 42)

  ;; apply more measurements to get a more interesting output
  ((compile-benchmark
    {:seed 0
     :tasks [{:t :call,
              :stage :foo
              :f (fn [_worker] (Thread/sleep 100))}]}
    ;; can use measurement to wrap stages/transactions with metrics
    @(requiring-resolve `xtdb.bench.measurement/wrap-task))
   42))
