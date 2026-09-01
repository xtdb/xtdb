package xtdb

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.binder.jvm.*
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.micrometer.tracing.Span
import io.micrometer.tracing.Tracer
import io.netty.util.internal.PlatformDependent
import org.apache.arrow.memory.AllocationListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import xtdb.util.info
import xtdb.util.logger
import java.lang.management.BufferPoolMXBean
import java.lang.management.ManagementFactory.getPlatformMXBeans
import java.util.concurrent.ConcurrentHashMap

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
    ): List<Gauge> = buildList {
        add(
            Gauge.builder("$name.allocated", al) { it.allocatedMemory.toDouble() }
                .baseUnit("bytes")
                .withTags(tags)
                .register(this@registerAllocatorGauges)
        )

        if (al.limit < Long.MAX_VALUE) {
            add(
                Gauge.builder("$name.limit", al) { it.limit.toDouble() }
                    .baseUnit("bytes")
                    .withTags(tags)
                    .register(this@registerAllocatorGauges)
            )
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
        private val registeredGauges = ConcurrentHashMap<BufferAllocator, List<Gauge>>()

        override fun onChildAdded(parent: BufferAllocator, child: BufferAllocator) {
            val parentParts = parent.name.split("/", limit = 2)
            val parentName = parentParts[0]
            val dbName = parentParts.getOrElse(1) { "" }
            val childName = child.name.split("/", limit = 2)[0]

            if ((parent is RootAllocator && childName != "database") || parentName == "database") {
                registeredGauges[child] = registerAllocatorGauges(child, "xtdb.allocator.memory", "allocator" to childName, "database" to dbName)
            }
        }

        override fun onChildRemoved(parent: BufferAllocator, child: BufferAllocator) {
            registeredGauges.remove(child)?.forEach { remove(it) }
        }
    }

    /**
     * Run [block] inside a tracing span named [name], tagging the span with [attributes].
     * A null receiver is a cheap no-op — [block] runs without any span machinery.
     *
     * Mirrors `xtdb.metrics/with-span`; keep them in sync until the remaining Clojure
     * callers move across.
     */
    inline fun <R> Tracer?.withSpan(
        name: String,
        parentSpan: Span? = null,
        attributes: Map<String, Any?> = emptyMap(),
        block: () -> R,
    ): R {
        // [block] is invoked exactly once so the inline expansion at each call site doesn't
        // duplicate the lambda body across the null-tracer / non-null-tracer branches.
        val tracer = this
        val span = tracer?.let { t ->
            (if (parentSpan != null) t.nextSpan(parentSpan) else t.nextSpan())
                ?.name(name)?.start()
                ?.also { sp -> for ((k, v) in attributes) sp.tag(k, v.toString()) }
        }
        val scope = if (tracer != null && span != null) tracer.withSpan(span) else null

        return try {
            block()
        } finally {
            scope?.close()
            span?.end()
        }
    }
}
