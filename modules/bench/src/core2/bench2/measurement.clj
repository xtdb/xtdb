(ns core2.bench2.measurement
  (:require [clojure.string :as str]
            [core2.bench2 :as b])
  (:import (java.time Duration)
           (io.micrometer.core.instrument MeterRegistry Meter Measurement Timer Gauge Tag)
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (io.micrometer.core.instrument.binder.jvm ClassLoaderMetrics JvmMemoryMetrics JvmHeapPressureMetrics JvmGcMetrics JvmThreadMetrics)
           (io.micrometer.core.instrument.binder.system ProcessorMetrics)
           (java.util ArrayList)
           (java.util.concurrent Executors TimeUnit)
           (java.util.function Supplier)
           (io.micrometer.core.instrument.binder MeterBinder)))

(def ^:dynamic *stage-reg* nil)

(defn meter-reg ^MeterRegistry []
  (let [meter-reg (SimpleMeterRegistry.)
        bind-metrics (fn [& metrics] (run! #(.bindTo ^MeterBinder % meter-reg) metrics))]
    (bind-metrics
     (ClassLoaderMetrics.)
     (JvmMemoryMetrics.)
     (JvmHeapPressureMetrics.)
     (JvmGcMetrics.)
     (ProcessorMetrics.)
     (JvmThreadMetrics.))
    meter-reg))

(defrecord MeterSample [meter time-ms statistic value])

(defn meter-sampler
  "Not thread safe, call to take a sample of meters in the given registry.
  Should be called on a sampler thread as part of a benchmark/load run.

  Can be called (sampler :summarize) for a data summary of the captured metric time series."
  [^MeterRegistry meter-reg]
  ;; most naive impl possible right now - can simply vary the sample rate according to duration / dimensionality
  ;; if memory is an issue
  (let [sample-list (ArrayList.)]
    (fn send-msg
      ([] (send-msg :sample))
      ([msg]
       (case msg
         :summarize
         (vec (for [[meter-name samples]
                    (group-by
                     (fn [{:keys [^Meter meter]}]
                       (-> meter .getId .getName))
                     sample-list)
                    [^Meter meter samples] (group-by :meter samples)
                    :let [series (->> meter .getId .getTagsAsIterable
                                      (map (fn [^Tag t] (str (.getKey t) "=" (.getValue t))))
                                      (str/join ", "))
                          series (not-empty series)]
                    [statistic samples] (group-by :statistic samples)]
                ;; todo count->rate automatically on non-neg deriv transform (can be a new 'statistic' dimension of counts)
                {:id (str/join " " [meter-name statistic series])
                 :metric (str/join " " [meter-name statistic])
                 :meter meter-name
                 :unit (if (= "count" statistic)
                         "count"
                         (-> meter .getId .getBaseUnit))
                 :series series
                 :statistic statistic
                 :samples (mapv (fn [{:keys [value, time-ms]}]
                                  {:value value
                                   :time-ms time-ms})
                                samples)}))
         :sample
         (let [time-ms (System/currentTimeMillis)]
           (doseq [^Meter meter (.getMeters meter-reg)
                   ^Measurement measurement (.measure meter)]
             (.add sample-list (->MeterSample meter time-ms (.getTagValueRepresentation (.getStatistic measurement)) (.getValue measurement))))))))))

(def percentiles
  [0.75 0.85 0.95 0.98 0.99 0.999])

(defn wrap-stage [task f]
  (let [stage (:stage task)
        ^Duration duration (:duration task)
        sample-freq
        (if duration
          (long (max 1000 (/ (* (.toMillis duration)) 120)))
          1000)]
    (fn instrumented-stage [worker]
      (let [reg (meter-reg)
            sampler (meter-sampler reg)
            executor (Executors/newSingleThreadScheduledExecutor)]
        (.scheduleAtFixedRate
         executor
         ^Runnable sampler
         0
         (long sample-freq)
         TimeUnit/MILLISECONDS)
        (try
          (let [start-ms (System/currentTimeMillis)]
            (binding [*stage-reg* reg] (f worker))
            (.shutdownNow executor)
            (when-not (.awaitTermination executor 1000 TimeUnit/MILLISECONDS)
              (throw (ex-info "Could not shut down sampler executor in time" {:stage stage})))
            (b/add-report worker {:stage stage,
                                  :start-ms start-ms
                                  :end-ms (System/currentTimeMillis)
                                  :metrics (sampler :summarize)}))
          (finally
            (.shutdownNow executor)))))))

(defn wrap-transaction [k f]
  (let [timer-delay
        (delay
          (when *stage-reg*
            (-> (Timer/builder (str "bench.transaction." (name k)))
                (.publishPercentiles (double-array percentiles))
                (.maximumExpectedValue (Duration/ofHours 8))
                (.minimumExpectedValue (Duration/ofNanos 1))
                (.register *stage-reg*))))]
    (fn instrumented-transaction [worker]
      (if-some [^Timer timer @timer-delay]
        (.recordCallable timer ^Callable (fn [] (f worker)))
        (f worker)))))

(defn wrap-task [task f]
  (let [{:keys [stage, transaction]} task]
    (cond
      stage (wrap-stage task f)
      transaction (wrap-transaction transaction f)
      :else f)))

(defn new-fn-gauge
  ([reg meter-name f] (new-fn-gauge reg meter-name f {}))
  ([^MeterRegistry reg meter-name f opts]
   (-> (Gauge/builder
        meter-name
        (reify Supplier
          (get [_] (f))))
       (cond-> (:unit opts) (.baseUnit (str (:unit opts))))
       (.register reg))))
