(ns xtdb.metrics
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (io.micrometer.core.instrument Counter Gauge MeterRegistry Timer Timer$Sample)
           (io.micrometer.core.instrument.binder MeterBinder)
           (io.micrometer.core.instrument.binder.jvm ClassLoaderMetrics JvmGcMetrics JvmHeapPressureMetrics JvmMemoryMetrics JvmThreadMetrics)
           (io.micrometer.core.instrument.binder.system ProcessorMetrics)
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (java.util.function Supplier)
           (java.util.stream Stream)
           (xtdb.api Xtdb$Config)
           (xtdb.api.metrics Metrics Metrics$Factory PrometheusMetrics$Factory)))

(defn add-counter [reg name {:keys [description]}]
  (cond-> (Counter/builder name)
    description (.description description)
    :always (.register reg)))

(def percentiles [0.75 0.85 0.95 0.98 0.99 0.999])

(defn add-timer [reg name {:keys [^String description]}]
  (cond-> (.. (Timer/builder name)
              (publishPercentiles (double-array percentiles)))
    description (.description description)
    :always (.register reg)))

(defmacro wrap-query [q timer]
  `(let [^Timer$Sample sample# (Timer/start)
         ^Stream stream# ~q]
     (.onClose stream# (fn [] (.stop sample# ~timer)))))

(defn add-gauge
  ([reg meter-name f] (add-gauge reg meter-name f {}))
  ([^MeterRegistry reg meter-name f opts]
   (-> (Gauge/builder
        meter-name
        (reify Supplier
          (get [_] (f))))
       (cond-> (:unit opts) (.baseUnit (str (:unit opts))))
       (.register reg))))

(defmethod xtn/apply-config! :xtdb.metrics/prometheus [^Xtdb$Config config _ {:keys [port], :or {port 8080}}]
  (.setMetrics config (PrometheusMetrics$Factory. port)))

(defmethod xtn/apply-config! :xtdb.metrics/registry [^Xtdb$Config config, _ [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :prometheus :xtdb.metrics/prometheus
                       :cloudwatch :xtdb.aws.cloudwatch/metrics
                       :azure-monitor :xtdb.azure.monitor/metrics)
                     opts))

(defmethod ig/init-key :xtdb.metrics/registry [_ ^Metrics$Factory factory]
  (let [^Metrics metrics (if factory
                           (.openMetrics factory)
                           (reify Metrics
                             (getRegistry [_]
                               (SimpleMeterRegistry.))

                             (close [_])))
        reg (.getRegistry metrics)]

    (doseq [^MeterBinder metric [(ClassLoaderMetrics.) (JvmMemoryMetrics.) (JvmHeapPressureMetrics.)
                                 (JvmGcMetrics.) (ProcessorMetrics.) (JvmThreadMetrics.)]]
      (.bindTo metric reg))

    metrics))

(defmethod ig/resolve-key :xtdb.metrics/registry [_ ^Metrics metrics]
  (.getRegistry metrics))

(defmethod ig/halt-key! :xtdb.metrics/registry [_ metrics]
  (util/close metrics))
