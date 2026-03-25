package xtdb

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.tracing.otel.bridge.OtelTracer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import xtdb.Metrics.registerRootAllocatorMeters
import xtdb.Metrics.rootAllocatorListener
import xtdb.Tracer.openTracer
import xtdb.api.Xtdb
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.util.closeAll
import xtdb.util.maxDirectMemory
import xtdb.util.safelyOpening

class NodeBase(
    val allocator: BufferAllocator, private val closeAllocator: Boolean, val config: Xtdb.Config,
    val memoryCache: MemoryCache, val diskCache: DiskCache?,
    val meterRegistry: MeterRegistry?, val tracer: OtelTracer?,
    val logClusters: Map<LogClusterAlias, Log.Cluster>,
) : AutoCloseable {

    override fun close() {
        logClusters.values.closeAll()
        memoryCache.close()
        if (closeAllocator) allocator.close()
    }

    companion object {
        @JvmStatic
        @JvmOverloads
        @JvmName("open")
        fun openBase(
            config: Xtdb.Config = Xtdb.Config(),
            openMeterRegistry: Boolean = true,
        ): NodeBase =
            safelyOpening {
                val externalAllocator = config.allocator
                val meterReg = if (openMeterRegistry) Metrics.openRegistry(config.nodeId) else null

                val al = externalAllocator
                    ?: open {
                        val limit = (maxDirectMemory * 0.9).toLong()
                        if (meterReg != null) {
                            RootAllocator(meterReg.rootAllocatorListener(), limit)
                                .also { meterReg.registerRootAllocatorMeters(it) }
                        } else {
                            RootAllocator(limit)
                        }
                    }

                val logClusters = config.logClusters.mapValues { (_, factory) -> open { factory.open() } }

                NodeBase(
                    allocator = al,
                    closeAllocator = externalAllocator == null,
                    config = config,
                    memoryCache = open { config.memoryCache.open(al, meterReg) },
                    diskCache = config.diskCache?.build(meterReg),
                    meterRegistry = meterReg,
                    tracer = config.tracer.openTracer(),
                    logClusters = logClusters,
                )
            }
    }
}
