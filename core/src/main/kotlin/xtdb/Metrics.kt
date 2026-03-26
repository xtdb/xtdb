package xtdb

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.binder.jvm.*
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.netty.util.internal.PlatformDependent
import org.apache.arrow.memory.AllocationListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
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

    private fun <T : Any> Gauge.Builder<T>.withTags(tags: Array<out Pair<String, String>>) = apply {
        for ((k, v) in tags) tag(k, v)
    }


    private fun MeterRegistry.registerAllocatorGauges(
        al: BufferAllocator, name: String, vararg tags: Pair<String, String>
    ) {
        Gauge.builder("$name.allocated", al) { it.allocatedMemory.toDouble() }
            .baseUnit("bytes")
            .withTags(tags)
            .register(this)

        if (al.limit < Long.MAX_VALUE) {
            Gauge.builder("$name.limit", al) { it.limit.toDouble() }
                .baseUnit("bytes")
                .withTags(tags)
                .register(this)
        }
    }

    @JvmStatic
    fun MeterRegistry.registerRootAllocatorMeters(alloc: RootAllocator) {
        registerAllocatorGauges(alloc, "xtdb.allocator.root.memory")
    }

    /**
     * Returns an AllocationListener that registers memory-usage meters for child allocators.
     * Database allocator is expected to be named 'database/<db-name>'.
     */
    @JvmStatic
    fun MeterRegistry.rootAllocatorListener() = object : AllocationListener {
        override fun onChildAdded(parent: BufferAllocator, child: BufferAllocator) {
            val parentParts = parent.name.split("/", limit = 2)
            val parentName = parentParts[0]
            val dbName = parentParts.getOrElse(1) { "" }
            val childName = child.name.split("/", limit = 2)[0]

            if ((parent is RootAllocator && childName != "database") || parentName == "database") {
                registerAllocatorGauges(child, "xtdb.allocator.memory", "database" to dbName)
            }
        }
    }
}
