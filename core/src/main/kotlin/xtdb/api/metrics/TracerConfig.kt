package xtdb.api.metrics

import io.opentelemetry.sdk.trace.SpanProcessor
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

@Serializable
data class TracerConfig(
    var enabled: Boolean = false,
    var endpoint: String = "http://localhost:4318/v1/traces",
    var serviceName: String = "xtdb",
    @Transient var spanProcessor: SpanProcessor? = null
) {
    fun enabled(enabled: Boolean) = apply { this.enabled = enabled }
    fun endpoint(endpoint: String) = apply { this.endpoint = endpoint }
    fun serviceName(serviceName: String) = apply { this.serviceName = serviceName }
    fun spanProcessor(spanProcessor: SpanProcessor) = apply { this.spanProcessor = spanProcessor }
}
