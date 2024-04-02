(ns xtdb.bench.measurement
  (:require [xtdb.bench :as b])
  (:import (java.time Duration)
           (io.micrometer.core.instrument Timer)))

(def ^:dynamic *registry* nil)

(def percentiles [0.75 0.85 0.95 0.98 0.99 0.999])

(defn wrap-stage [task f]
  (let [stage (:stage task)]
    (fn instrumented-stage [worker]
      (let [start-ms (System/currentTimeMillis)]
        (f worker)
        (b/log-report worker {:stage stage,
                              :time-taken-ms (- (System/currentTimeMillis) start-ms)})))))

(defn wrap-transaction [{:keys [transaction labels] :as _task} f]
  (let [timer-delay
        (delay
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

(defn wrap-task [task f]
  (let [{:keys [stage, transaction]} task]
    (cond
      stage (wrap-stage task f)
      transaction (wrap-transaction task f)
      :else f)))
