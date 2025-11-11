@file:UseSerializers(ZoneIdSerde::class)

package xtdb.api

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.ZoneIdSerde
import xtdb.api.Authenticator.Factory.UserTable
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.metrics.HealthzConfig
import xtdb.api.metrics.TracerConfig
import xtdb.api.module.XtdbModule
import xtdb.api.storage.Storage
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.database.Database
import xtdb.util.requiringResolve
import java.nio.file.Files
import java.nio.file.Path
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID.randomUUID
import kotlin.io.path.extension

interface Xtdb : DataSource, AutoCloseable {

    val serverPort: Int
    val serverReadOnlyPort: Int

    interface CompactorNode : AutoCloseable
    interface XtdbInternal : Xtdb {
        val dbCatalog: Database.Catalog
    }

    fun <T : XtdbModule> module(type: Class<T>): T?

    fun addMeterRegistry(meterRegistry: MeterRegistry)

    @Serializable
    data class Config(
        var server: ServerConfig? = ServerConfig(),
        var logClusters: Map<LogClusterAlias, Log.Cluster.Factory<*>> = emptyMap(),
        var log: Log.Factory = Log.inMemoryLog,
        var storage: Storage.Factory = Storage.inMemory(),
        val memoryCache: MemoryCache.Factory = MemoryCache.Factory(),
        var diskCache: DiskCache.Factory? = null,
        var healthz: HealthzConfig? = null,
        var defaultTz: ZoneId = ZoneOffset.UTC,
        val indexer: IndexerConfig = IndexerConfig(),
        val compactor: CompactorConfig = CompactorConfig(),
        var authn: Authenticator.Factory = UserTable(),
        var garbageCollector: GarbageCollectorConfig = GarbageCollectorConfig(),
        var tracer: TracerConfig = TracerConfig(),
        var nodeId: String = System.getenv("XTDB_NODE_ID") ?: randomUUID().toString().takeWhile { it != '-' }
    ) {
        private val modules: MutableList<XtdbModule.Factory> = mutableListOf()

        fun logClusters(clusters: Map<LogClusterAlias, Log.Cluster.Factory<*>>) = apply { logClusters += clusters }

        fun logCluster(alias: LogClusterAlias, cluster: Log.Cluster.Factory<*>) =
            apply { logClusters += alias to cluster }

        fun log(log: Log.Factory) = apply { this.log = log }
        fun storage(storage: Storage.Factory) = apply { this.storage = storage }

        fun diskCache(diskCache: DiskCache.Factory?) = apply { this.diskCache = diskCache }

        @JvmSynthetic
        fun server(configure: ServerConfig.() -> Unit) = apply { (server ?: ServerConfig()).configure() }

        @JvmSynthetic
        fun indexer(configure: IndexerConfig.() -> Unit) = apply { indexer.configure() }

        @JvmSynthetic
        fun compactor(configure: CompactorConfig.() -> Unit) = apply { compactor.configure() }

        fun healthz(healthz: HealthzConfig) = apply { this.healthz = healthz }

        fun tracer(tracer: TracerConfig) = apply { this.tracer = tracer }

        @JvmSynthetic
        fun tracer(configure: TracerConfig.() -> Unit) = tracer(TracerConfig().also(configure))

        fun garbageCollector(garbageCollector: GarbageCollectorConfig) =
            apply { this.garbageCollector = garbageCollector }

        @JvmSynthetic
        fun garbageCollector(configure: GarbageCollectorConfig.() -> Unit) =
            garbageCollector(GarbageCollectorConfig().also(configure))

        fun defaultTz(defaultTz: ZoneId) = apply { this.defaultTz = defaultTz }

        fun authn(authn: Authenticator.Factory) = apply { this.authn = authn }

        fun nodeId(nodeId: String) = apply { this.nodeId = nodeId }

        fun getModules(): List<XtdbModule.Factory> = modules
        fun module(module: XtdbModule.Factory) = apply { this.modules += module }
        fun modules(vararg modules: XtdbModule.Factory) = apply { this.modules += modules }
        fun modules(modules: List<XtdbModule.Factory>) = apply { this.modules += modules }

        fun open(): Xtdb = requiringResolve("xtdb.node.impl/open-node").invoke(this) as Xtdb

        fun openCompactor() = requiringResolve("xtdb.node.impl/open-compactor").invoke(this) as CompactorNode
    }

    companion object {
        internal interface XtdbInternal {
            val dbCatalog: Database.Catalog
        }

        @JvmStatic
        fun configure() = Config()

        @JvmStatic
        fun readConfig(path: Path): Config {
            if (path.extension != "yaml") {
                throw IllegalArgumentException("Invalid config file type - must be '.yaml'")
            } else if (!path.toFile().exists()) {
                throw IllegalArgumentException("Provided config file does not exist")
            }

            val yamlString = Files.readString(path)
            return nodeConfig(yamlString)
        }

        @JvmStatic
        @JvmOverloads
        fun openNode(config: Config = Config()) = config.open()

        /**
         * Opens a node using a YAML configuration file - will throw an exception if the specified path does not exist
         * or is not a valid `.yaml` extension file.
         */
        @JvmStatic
        fun openNode(path: Path): Xtdb = readConfig(path).open()

        @JvmSynthetic
        fun openNode(configure: Config.() -> Unit) = openNode(Config().also(configure))
    }
}

inline fun <reified T : XtdbModule> Xtdb.module(): T? = module(T::class.java)
