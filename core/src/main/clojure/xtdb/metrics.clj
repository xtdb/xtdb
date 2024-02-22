(ns xtdb.metrics
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.util :as util])
  (:import (com.sun.net.httpserver HttpServer HttpHandler)
           (java.net InetSocketAddress)
           (java.util.function Supplier)
           (java.util.stream Stream)
           (io.micrometer.core.instrument MeterRegistry Meter Measurement Timer Gauge Tag Counter Timer$Sample)
           (io.micrometer.core.instrument.binder MeterBinder)
           (io.micrometer.core.instrument.binder.jvm ClassLoaderMetrics JvmMemoryMetrics JvmHeapPressureMetrics JvmGcMetrics JvmThreadMetrics)
           (io.micrometer.core.instrument.binder.system ProcessorMetrics)
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (io.micrometer.prometheus PrometheusMeterRegistry PrometheusConfig)))

(defn meter-reg ^MeterRegistry
  ([] (meter-reg (SimpleMeterRegistry.)))
  ([meter-reg]
   (doseq [^MeterBinder metric [(ClassLoaderMetrics.) (JvmMemoryMetrics.) (JvmHeapPressureMetrics.)
                                (JvmGcMetrics.) (ProcessorMetrics.) (JvmThreadMetrics.)]]
     (.bindTo metric meter-reg))
   meter-reg))

(defmethod ig/init-key :xtdb/meter-registry [_ {}]
  (meter-reg (PrometheusMeterRegistry. PrometheusConfig/DEFAULT)))

(defmethod ig/halt-key! :xtdb/meter-registry [_ ^MeterRegistry registry]
  (.close registry))

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

(defn wrap-stream [^Stream stream ^Timer timer ^MeterRegistry registry]
  (let [^Timer$Sample sample (Timer/start registry)]
    (.onClose stream (fn [] (.stop sample timer)))))

(defn add-gauge
  ([reg meter-name f] (add-gauge reg meter-name f {}))
  ([^MeterRegistry reg meter-name f opts]
   (-> (Gauge/builder
        meter-name
        (reify Supplier
          (get [_] (f))))
       (cond-> (:unit opts) (.baseUnit (str (:unit opts))))
       (.register reg))))


(defmethod ig/prep-key :xtdb/metrics-server [_ opts]
  (merge {:registry (ig/ref :xtdb/meter-registry)}
         opts))

(defmethod ig/init-key :xtdb/metrics-server [_ {:keys [^PrometheusMeterRegistry registry port] :or {port 8080}}]
  (try
    (let [port (if (util/port-free? port) port (util/free-port))
          http-server (HttpServer/create (InetSocketAddress. port) 0)]
      (.createContext http-server "/metrics"
                      (reify HttpHandler
                        (handle [_this exchange]
                          (let [response (.getBytes (.scrape registry))]
                            (.sendResponseHeaders exchange 200 (count response))
                            (with-open [os (.getResponseBody exchange)]
                              (.write os response))))))
      (.start http-server)
      (log/debug "Metrics server started on port: " port)
      http-server)
    (catch java.io.IOException e
      (throw (err/runtime-err :metrics-server-error {} e)))))

(defmethod ig/halt-key! :xtdb/metrics-server [_ ^HttpServer server]
  (.stop server 0)
  (log/debug "Metrics server stopped."))
