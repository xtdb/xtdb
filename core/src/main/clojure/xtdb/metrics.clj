(ns xtdb.metrics
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.node :as xtn])
  (:import (io.micrometer.core.instrument Counter Gauge Gauge$Builder MeterRegistry Timer Timer$Sample)
           (io.micrometer.prometheusmetrics PrometheusMeterRegistry)
           (io.micrometer.tracing Tracer Span)
           (java.util.stream Stream)
           (org.apache.arrow.memory BufferAllocator)
           xtdb.Metrics))

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
  ([reg meter-name f] (add-gauge reg meter-name f {}))
  ([^MeterRegistry reg meter-name f {:keys [unit tag]}]
   (let [[tag-key tag-value] tag]
     (-> (Gauge/builder meter-name f)
         (cond->
             unit (.baseUnit (str unit))
             tag (.tag tag-key tag-value))
         (.register reg)))))

(defn- register-gauge! [^MeterRegistry reg, ^Gauge$Builder g]
  (doto (.register g reg)
    (as-> ^Gauge registered
      (log/debug "Registered allocator gauge:" (.getId registered)))))

(defn- allocator-gauge-builders [^BufferAllocator alloc, name]
  (concat [(-> (Gauge/builder (str name ".allocated") #(.getAllocatedMemory alloc))
               (.baseUnit "bytes"))]
          (when (< (.getLimit alloc) Long/MAX_VALUE)
            [(-> (Gauge/builder (str name ".limit") #(.getLimit alloc))
                 (.baseUnit "bytes"))])))

(defn register-root-allocator-meters! [^MeterRegistry reg, ^BufferAllocator alloc]
  (doseq [^Gauge$Builder g (allocator-gauge-builders alloc "xtdb.allocator.root.memory")]
    (register-gauge! reg g)))

(defn root-allocator-listener
  "Registers exhaustive and exclusive memory-usage meters for certain RootAllocator children.
   Database allocator is expected to be named 'database/<db-name>'"
  [^MeterRegistry reg]
  ; An allocator listener with a naming convention for allocators is enough for now.
  ; A BufferAllocator wrapper would enable explicit child allocator settings at their creation site.
  (reify org.apache.arrow.memory.AllocationListener
    (^void onChildAdded [_, ^BufferAllocator parent, ^BufferAllocator child]
      (let [[parent-name db-name] (str/split (.getName parent) #"/" 2)
            [child-name] (str/split (.getName child) #"/" 2)]
        (when (or (and (instance? org.apache.arrow.memory.RootAllocator parent)
                       (not= child-name "database"))
                  (= parent-name "database"))
          (doseq [g (allocator-gauge-builders child "xtdb.allocator.memory")]
            (register-gauge! reg (-> g
                                     (.tag "allocator" child-name)
                                     (.tag "database" (or db-name ""))))))))))

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

(defmethod ig/expand-key ::registry [k _]
  {k {:config (ig/ref :xtdb/config)}})

(defmethod ig/init-key ::registry [_ {{:keys [node-id]} :config}]
  (Metrics/openRegistry node-id))

(defmethod ig/halt-key! ::registry [_ ^PrometheusMeterRegistry reg]
  (.clear reg)
  (.close reg))

(defmethod xtn/apply-config! ::cloudwatch [config _k v]
  (xtn/apply-config! config :xtdb.aws.cloudwatch/metrics v))

(defmethod xtn/apply-config! ::azure-monitor [config _k v]
  (xtn/apply-config! config :xtdb.azure.monitor/metrics v))
