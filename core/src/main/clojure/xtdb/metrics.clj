(ns xtdb.metrics
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (io.micrometer.core.instrument Counter Gauge MeterRegistry Tag Timer Timer$Sample)
           (io.micrometer.core.instrument.binder MeterBinder)
           (io.micrometer.core.instrument.binder.jvm ClassLoaderMetrics JvmGcMetrics JvmHeapPressureMetrics JvmMemoryMetrics JvmThreadMetrics)
           (io.micrometer.core.instrument.binder.system ProcessorMetrics)
           (io.micrometer.prometheusmetrics PrometheusConfig PrometheusMeterRegistry)
           java.util.List
           (java.util.stream Stream)
           (org.apache.arrow.memory BufferAllocator)))

(defn add-counter
  (^io.micrometer.core.instrument.Counter [reg name] (add-counter reg name {}))
  (^io.micrometer.core.instrument.Counter [reg name {:keys [description]}]
   (cond-> (Counter/builder name)
     description (.description description)
     :always (.register reg))))

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
  ([reg meter-name f] (add-gauge reg meter-name f {}))
  ([^MeterRegistry reg meter-name f {:keys [unit tag]}]
   (let [[tag-key tag-value] tag]
     (-> (Gauge/builder meter-name f)
         (cond->
             unit (.baseUnit (str unit))
             tag (.tag tag-key tag-value))
         (.register reg)))))

(defn add-allocator-gauge [reg meter-name ^BufferAllocator allocator]
  (add-gauge reg meter-name (fn [] (.getAllocatedMemory allocator)) {:unit "bytes"}))

(defn direct-memory-pool ^java.lang.management.BufferPoolMXBean []
  (->> (java.lang.management.ManagementFactory/getPlatformMXBeans java.lang.management.BufferPoolMXBean)
       (some #(when (= (.getName ^java.lang.management.BufferPoolMXBean %) "direct") %))))

(defmethod ig/prep-key ::registry [_ _]
  {:config (ig/ref :xtdb/config)})

(defmethod ig/init-key ::registry [_ {{:keys [node-id]} :config}]
  (let [reg (PrometheusMeterRegistry. PrometheusConfig/DEFAULT)]

    ;; Add common tag for the node
    (let [^List tags [(Tag/of "node-id" node-id)]]
      (log/infof "tagging all metrics with node-id: %s" node-id)
      (-> (.config reg)
          (.commonTags tags)))

    (doseq [^MeterBinder metric [(ClassLoaderMetrics.) (JvmMemoryMetrics.) (JvmHeapPressureMetrics.)
                                 (JvmGcMetrics.) (ProcessorMetrics.) (JvmThreadMetrics.)]]
      (.bindTo metric reg))

    (add-gauge reg "jvm.memory.netty.bytes" #(util/used-netty-memory) {:unit "bytes"})

    (when-let [direct-pool (direct-memory-pool)]
      (add-gauge reg "jvm.memory.direct.bytes"
                 #(.getMemoryUsed direct-pool)
                 {:unit "bytes"}))
    reg))


(defmethod xtn/apply-config! ::cloudwatch [config _k v]
  (xtn/apply-config! config :xtdb.aws.cloudwatch/metrics v))

(defmethod xtn/apply-config! ::azure-monitor [config _k v]
  (xtn/apply-config! config :xtdb.azure.monitor/metrics v))
