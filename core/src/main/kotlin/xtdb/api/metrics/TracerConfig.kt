@file:UseSerializers(StringWithEnvVarSerde::class)

package xtdb.api.metrics

import io.opentelemetry.sdk.trace.SpanProcessor
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.IntWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde

@Serializable
data class TracerConfig(
    var enabled: Boolean = false,
    var endpoint: String = "http://localhost:4318/v1/traces",
    var serviceName: String = "xtdb",
    var queryTracing: Boolean = true,
    var transactionTracing: Boolean = true,
    @Transient var spanProcessor: SpanProcessor? = null
) {
    fun enabled(enabled: Boolean) = apply { this.enabled = enabled }
    fun endpoint(endpoint: String) = apply { this.endpoint = endpoint }
    fun serviceName(serviceName: String) = apply { this.serviceName = serviceName }
    fun queryTracing(queryTracing: Boolean) = apply { this.queryTracing = queryTracing }
    fun transactionTracing(transactionTracing: Boolean) = apply { this.transactionTracing = transactionTracing }
    fun spanProcessor(spanProcessor: SpanProcessor) = apply { this.spanProcessor = spanProcessor }
}
