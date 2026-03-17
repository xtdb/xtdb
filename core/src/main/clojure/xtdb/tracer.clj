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
           (xtdb.api.metrics TracerConfig)
           xtdb.Tracer))

(defmethod xtn/apply-config! :xtdb/tracer [^Xtdb$Config config _ {:keys [enabled? endpoint service-name query-tracing? transaction-tracing? span-processor]}]
  (.tracer config
           (cond-> (TracerConfig.)
             (some? enabled?) (.enabled enabled?)
             endpoint (.endpoint endpoint)
             service-name (.serviceName service-name)
             (some? query-tracing?) (.queryTracing query-tracing?)
             (some? transaction-tracing?) (.transactionTracing transaction-tracing?)
             span-processor (.spanProcessor span-processor))))

(defmethod ig/expand-key :xtdb/tracer [k ^TracerConfig config]
  {k {:tracer-config config
      :enabled? (.getEnabled config)
      :query-tracing? (.getQueryTracing config)
      :transaction-tracing? (.getTransactionTracing config)
      :config (ig/ref :xtdb/config)}})

(defmethod ig/init-key :xtdb/tracer [_ {:keys [enabled? tracer-config query-tracing? transaction-tracing?]}]
  {:tracer (when enabled?
             (Tracer/open tracer-config))
   :query-tracing? query-tracing?
   :transaction-tracing? transaction-tracing?})
