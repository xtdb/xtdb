package xtdb

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.tracing.otel.bridge.OtelTracer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import xtdb.Metrics.registerRootAllocatorMeters
import xtdb.Metrics.rootAllocatorListener
import xtdb.Tracer.openTracer
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.Xtdb
import xtdb.cache.DiskCache
import xtdb.api.error.Incorrect
import xtdb.cache.MemoryCache
import xtdb.compactor.Compactor
import xtdb.query.IQuerySource
import xtdb.util.closeAll
import xtdb.util.maxDirectMemory
import xtdb.util.requiringResolve
import xtdb.util.safelyOpening

class NodeBase(
    val allocator: BufferAllocator, private val closeAllocator: Boolean, val config: Xtdb.Config,
    val memoryCache: MemoryCache, val diskCache: DiskCache?,
    val meterRegistry: MeterRegistry?, val tracer: OtelTracer?,
    val remotes: Map<RemoteAlias, Remote>,
    val compactor: Compactor,
    val querySource: IQuerySource,
) : AutoCloseable {

    override fun close() {
        compactor.close()
        querySource.close()
        remotes.values.closeAll()
        memoryCache.close()
        if (closeAllocator) allocator.close()
    }

    companion object {
        private val compactorFactory by lazy {
            requiringResolve("xtdb.compactor/->factory").invoke() as Compactor.Factory
        }

        private val infoSchemaFactory by lazy { requiringResolve("xtdb.information-schema/->info-schema") }
        private val scanEmitterFactory by lazy { requiringResolve("xtdb.operator.scan/->scan-emitter") }
        private val querySourceFactory by lazy {
            requiringResolve("xtdb.query/->factory").invoke() as IQuerySource.Factory
        }

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

                val collisions = config.logClusters.keys.intersect(config.remotes.keys)
                if (collisions.isNotEmpty()) {
                    throw Incorrect(
                        "alias appears under both 'logClusters:' and 'remotes:': ${collisions.sorted().joinToString()}",
                        errorCode = "xtdb/duplicate-remote-alias",
                        data = mapOf("aliases" to collisions.sorted()),
                    )
                }

                val remotes: Map<RemoteAlias, Remote> =
                    (config.logClusters + config.remotes)
                        .mapValues { (_, factory) -> open { factory.open() } }

                val compactor = open { compactorFactory.create(meterReg, config.compactor.threads) }

                val infoSchema = infoSchemaFactory.invoke(al, meterReg, config.authn)
                val scanEmitter = scanEmitterFactory.invoke(infoSchema)
                val querySource = open { querySourceFactory.create(al, meterReg, scanEmitter) }

                NodeBase(
                    allocator = al,
                    closeAllocator = externalAllocator == null,
                    config = config,
                    memoryCache = open { config.memoryCache.open(al, meterReg) },
                    diskCache = config.diskCache?.build(meterReg),
                    meterRegistry = meterReg,
                    tracer = config.tracer.openTracer(),
                    remotes = remotes,
                    compactor = compactor,
                    querySource = querySource,
                )
            }
    }
}
