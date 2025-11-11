(ns xtdb.tracer
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.node :as xtn])
  (:import (io.opentelemetry.api.common Attributes)
           (io.opentelemetry.exporter.otlp.http.trace OtlpHttpSpanExporter)
           (io.opentelemetry.sdk OpenTelemetrySdk)
           (io.opentelemetry.sdk.resources Resource)
           (io.opentelemetry.sdk.trace SdkTracerProvider)
           (io.opentelemetry.sdk.trace.export SimpleSpanProcessor)
           (io.micrometer.tracing.otel.bridge OtelTracer OtelCurrentTraceContext OtelTracer$EventPublisher)
           (xtdb.api Xtdb$Config)
           (xtdb.api.metrics TracerConfig)))

(defn noop-event-publisher []
  (reify OtelTracer$EventPublisher
    (publishEvent [_ _])))

(defn create-tracer [{:keys [enabled? endpoint ^String service-name span-processor]}]
  (when enabled?
    (try
      (let [resource (-> (Resource/getDefault)
                         (.merge (Resource/create
                                  (-> (Attributes/builder)
                                      (.put "service.name" service-name)
                                      (.build)))))
            processor (or span-processor
                          (let [builder (cond-> (OtlpHttpSpanExporter/builder)
                                          endpoint (.setEndpoint endpoint))]
                            (SimpleSpanProcessor/create (.build builder))))
            tracer-provider (-> (SdkTracerProvider/builder)
                                (.setResource resource)
                                (.addSpanProcessor processor)
                                (.build))
            otel-sdk (-> (OpenTelemetrySdk/builder)
                         (.setTracerProvider tracer-provider)
                         (.build))
            otel-tracer (.getTracer otel-sdk "xtdb")
            current-ctx (OtelCurrentTraceContext.)
            event-pub (noop-event-publisher)]

        (log/infof "OpenTelemetry tracer created for service: %s" service-name)
        (OtelTracer. otel-tracer current-ctx event-pub))

      (catch Exception e
        (log/warnf e "Failed to create OpenTelemetry tracer, tracing will be disabled")
        nil))))

(defmethod xtn/apply-config! :xtdb/tracer [^Xtdb$Config config _ {:keys [enabled? endpoint service-name span-processor]}]
  (.tracer config
           (cond-> (TracerConfig.) 
             (some? enabled?) (.enabled enabled?)
             endpoint (.endpoint endpoint)
             service-name (.serviceName service-name)
             span-processor (.spanProcessor span-processor))))

(defmethod ig/expand-key :xtdb/tracer [k ^TracerConfig config]
  {k {:enabled? (.getEnabled config)
      :endpoint (.getEndpoint config)
      :service-name (.getServiceName config)
      :span-processor (.getSpanProcessor config)
      :config (ig/ref :xtdb/config)}})

(defmethod ig/init-key :xtdb/tracer [_ {:keys [enabled? endpoint service-name span-processor] {:keys [node-id]} :config}]
  (let [service-name (or service-name (str "xtdb-" node-id))]
    (create-tracer {:enabled? enabled?
                    :endpoint endpoint
                    :service-name service-name
                    :span-processor span-processor})))
