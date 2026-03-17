package xtdb

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.binder.jvm.*
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.netty.util.internal.PlatformDependent
import xtdb.util.info
import xtdb.util.logger
import java.lang.management.BufferPoolMXBean
import java.lang.management.ManagementFactory.getPlatformMXBeans

object Metrics {
    private val LOG = Metrics::class.logger

    @JvmStatic
    fun openRegistry(nodeId: String): MeterRegistry =
        PrometheusMeterRegistry(PrometheusConfig.DEFAULT).also { reg ->
            LOG.info("tagging all metrics with node-id: $nodeId")
            reg.config().commonTags(listOf(Tag.of("node-id", nodeId)))

            ClassLoaderMetrics().bindTo(reg)
            JvmMemoryMetrics().bindTo(reg)
            JvmHeapPressureMetrics().bindTo(reg)
            JvmGcMetrics().bindTo(reg)
            ProcessorMetrics().bindTo(reg)
            JvmThreadMetrics().bindTo(reg)

            Gauge.builder("jvm.memory.netty.bytes") { PlatformDependent.usedDirectMemory().toDouble() }
                .baseUnit("bytes")
                .register(reg)

            getPlatformMXBeans(BufferPoolMXBean::class.java)
                .firstOrNull { it.name == "direct" }
                ?.let { pool ->
                    Gauge.builder("jvm.memory.direct.bytes") { pool.memoryUsed.toDouble() }
                        .baseUnit("bytes")
                        .register(reg)
                }
        }
}
