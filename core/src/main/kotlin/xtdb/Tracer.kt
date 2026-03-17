package xtdb

import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext
import io.micrometer.tracing.otel.bridge.OtelTracer
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import xtdb.api.metrics.TracerConfig
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn

object Tracer {
    private val LOG = Tracer::class.logger

    @JvmStatic
    @JvmName("open")
    fun TracerConfig.openTracer(): OtelTracer? = try {
        val resource =
            Resource.getDefault().merge(Resource.create(Attributes.builder().put("service.name", serviceName).build()))

        val spanProcessor = spanProcessor
            ?: SimpleSpanProcessor.create(OtlpHttpSpanExporter.builder().setEndpoint(endpoint).build())

        val tracerProvider =
            SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(spanProcessor)
                .build()

        val tracer =
            OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build()
                .getTracer("xtdb")

        LOG.info("OpenTelemetry tracer created for service: $serviceName")

        OtelTracer(tracer, OtelCurrentTraceContext()) { }
    } catch (e: Throwable) {
        LOG.warn(e, "Failed to create OpenTelemetry tracer, tracing will be disabled")
        null
    }
}
