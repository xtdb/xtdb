package xtdb.api.metrics

import io.micrometer.core.instrument.MeterRegistry

interface Metrics : AutoCloseable {

    val registry: MeterRegistry

    override fun close() {}

    interface Factory {
        fun openMetrics(): Metrics
    }
}