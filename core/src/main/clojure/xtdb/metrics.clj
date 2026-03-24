(ns xtdb.metrics
  (:require [xtdb.node :as xtn])
  (:import (io.micrometer.core.instrument Counter Gauge MeterRegistry Timer Timer$Sample)
           (io.micrometer.tracing Tracer Span)
           (java.util.stream Stream)))

(defn add-counter
  (^io.micrometer.core.instrument.Counter [reg name] (add-counter reg name {}))
  (^io.micrometer.core.instrument.Counter [reg name {:keys [description]}]
   (cond-> (Counter/builder name)
     description (.description description)
     :always (.register reg))))

(defn inc-counter! [^Counter counter]
  (when counter
    (.increment counter)))

(defmacro record-callable! [timer & body]
  `(if-let [^Timer timer# ~timer]
     (.recordCallable timer# (fn [] ~@body))
     (do ~@body)))

(def percentiles [0.75 0.85 0.95 0.98 0.99 0.999])

(defn add-timer ^io.micrometer.core.instrument.Timer [reg name {:keys [^String description]}]
  (cond-> (.. (Timer/builder name)
              (publishPercentiles (double-array percentiles)))
    description (.description description)
    :always (.register reg)))

(defmacro wrap-query [q timer]
  `(let [^Timer$Sample sample# (Timer/start)
         ^Stream stream# ~q]
     (.onClose stream# (fn [] (.stop sample# ~timer)))))

(defn add-gauge
  ([reg meter-name obj f] (add-gauge reg meter-name obj f {}))
  ([^MeterRegistry reg meter-name obj f {:keys [unit tag]}]
   (let [[tag-key tag-value] tag]
     (-> (Gauge/builder meter-name obj f)
         (cond->
             unit (.baseUnit (str unit))
             tag (.tag tag-key tag-value))
         (.register reg)))))

(defn start-span ^Span [^Tracer tracer ^String span-name {:keys [^Span parent-span attributes]}]
  (let [span-builder (if parent-span
                       (.nextSpan tracer parent-span)
                       (.nextSpan tracer))
        span (-> span-builder
                 (.name span-name)
                 (.start))]
    (doseq [[k v] attributes]
      (.tag span (name k) (str v)))
    span))

(defn end-span [^Span span]
  (.end span))

(defmacro with-span [tracer span-name opts & body]
  `(if-let [tracer# ~tracer]
     (let [span# (start-span tracer# ~span-name ~opts)]
       (try
         (with-open [_# (.withSpan tracer# span#)]
           ~@body)
         (finally
           (end-span span#))))
     (do ~@body)))

(defmethod xtn/apply-config! ::cloudwatch [config _k v]
  (xtn/apply-config! config :xtdb.aws.cloudwatch/metrics v))

(defmethod xtn/apply-config! ::azure-monitor [config _k v]
  (xtn/apply-config! config :xtdb.azure.monitor/metrics v))
