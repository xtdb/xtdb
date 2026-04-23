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
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.compactor.Compactor
import xtdb.indexer.Indexer
import xtdb.query.IQuerySource
import xtdb.util.closeAll
import xtdb.util.maxDirectMemory
import xtdb.util.requiringResolve
import xtdb.util.safelyOpening

class NodeBase(
    val allocator: BufferAllocator, private val closeAllocator: Boolean, val config: Xtdb.Config,
    val memoryCache: MemoryCache, val diskCache: DiskCache?,
    val meterRegistry: MeterRegistry?, val tracer: OtelTracer?,
    val logClusters: Map<LogClusterAlias, Log.Cluster>,
    val remotes: Map<RemoteAlias, Remote>,
    val compactor: Compactor,
    val querySource: IQuerySource,
    val indexerFactory: Indexer.Factory,
) : AutoCloseable {

    override fun close() {
        compactor.close()
        querySource.close()
        remotes.values.closeAll()
        logClusters.values.closeAll()
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

        private val idxFactory by lazy {
            requiringResolve("xtdb.indexer/->factory").invoke() as Indexer.Factory
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

                val logClusters = config.logClusters.mapValues { (_, factory) -> open { factory.open() } }
                val remotes = config.remotes.mapValues { (_, factory) -> open { factory.open() } }

                val compactor = open { compactorFactory.create(meterReg, config.compactor.threads) }

                val infoSchema = infoSchemaFactory.invoke(al, meterReg)
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
                    logClusters = logClusters,
                    remotes = remotes,
                    compactor = compactor,
                    querySource = querySource,
                    indexerFactory = idxFactory,
                )
            }
    }
}
