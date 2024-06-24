(ns xtdb.metrics
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (com.sun.net.httpserver HttpHandler HttpServer)
           (io.micrometer.cloudwatch2 CloudWatchConfig CloudWatchMeterRegistry)
           (io.micrometer.core.instrument Clock Counter Gauge MeterRegistry Timer Timer$Sample)
           (io.micrometer.core.instrument.binder MeterBinder)
           (io.micrometer.core.instrument.binder.jvm ClassLoaderMetrics JvmGcMetrics JvmHeapPressureMetrics JvmMemoryMetrics JvmThreadMetrics)
           (io.micrometer.core.instrument.binder.system ProcessorMetrics)
           (io.micrometer.core.instrument.composite CompositeMeterRegistry)
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (io.micrometer.prometheus PrometheusConfig PrometheusMeterRegistry)
           (java.net InetSocketAddress)
           (java.util.function Supplier)
           (java.util.stream Stream)
           (software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient)
           (xtdb.api MetricsConfig Xtdb$Config)))

(defn meter-reg ^MeterRegistry
  ([] (meter-reg (SimpleMeterRegistry.)))
  ([meter-reg]
   (doseq [^MeterBinder metric [(ClassLoaderMetrics.) (JvmMemoryMetrics.) (JvmHeapPressureMetrics.)
                                (JvmGcMetrics.) (ProcessorMetrics.) (JvmThreadMetrics.)]]
     (.bindTo metric meter-reg))
   meter-reg))

(defn- aws-env-var? [s]
  (str/starts-with? s "AWS"))

(defmethod ig/init-key :xtdb/meter-registry [_ {}]
  (let [composite-reg (cond-> (CompositeMeterRegistry.)
                        true
                        (.add (PrometheusMeterRegistry. PrometheusConfig/DEFAULT))

                        (some aws-env-var? (keys (System/getenv)))
                        (.add (CloudWatchMeterRegistry.
                               (reify CloudWatchConfig
                                 (get [_ _s] nil)
                                 (namespace [_] "xtdb.metrics"))
                               Clock/SYSTEM
                               (CloudWatchAsyncClient/create))))]

    (meter-reg composite-reg)))

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

(defmacro wrap-query [q timer registry]
  (let [registry (vary-meta registry assoc :tag `MeterRegistry)]
    `(let [^Timer$Sample sample# (Timer/start ~registry)
           ^Stream stream# ~q]
       (.onClose stream# (fn [] (.stop sample# ~timer))))))

(defn add-gauge
  ([reg meter-name f] (add-gauge reg meter-name f {}))
  ([^MeterRegistry reg meter-name f opts]
   (-> (Gauge/builder
        meter-name
        (reify Supplier
          (get [_] (f))))
       (cond-> (:unit opts) (.baseUnit (str (:unit opts))))
       (.register reg))))

(defmethod xtn/apply-config! :xtdb/metrics-server [^Xtdb$Config config, _ {:keys [port], :or {port 8080}}]
  (.setMetrics config (MetricsConfig. port)))

(defmethod ig/prep-key :xtdb/metrics-server [_ ^MetricsConfig opts]
  {:registry (ig/ref :xtdb/meter-registry)
   :port (.getPort opts)})

(defmethod ig/init-key :xtdb/metrics-server [_ {:keys [^CompositeMeterRegistry registry port]}]
  (try
    (let [^PrometheusMeterRegistry prometheus-reg (->> (.getRegistries registry)
                                                       (filter #(instance? PrometheusMeterRegistry %))
                                                       first)
          port (if (util/port-free? port) port (util/free-port))
          http-server (HttpServer/create (InetSocketAddress. port) 0)]
      (.createContext http-server "/metrics"
                      (reify HttpHandler
                        (handle [_this exchange]
                          (let [response (.getBytes (.scrape prometheus-reg))]
                            (.sendResponseHeaders exchange 200 (count response))
                            (with-open [os (.getResponseBody exchange)]
                              (.write os response))))))
      (.start http-server)
      (log/info "Metrics server started on port: " port)
      http-server)
    (catch java.io.IOException e
      (throw (err/runtime-err :metrics-server-error {} e)))))

(defmethod ig/halt-key! :xtdb/metrics-server [_ ^HttpServer server]
  (.stop server 0)
  (log/info "Metrics server stopped."))
