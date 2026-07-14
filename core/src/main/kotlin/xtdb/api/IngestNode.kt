package xtdb.api

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.api.metrics.HealthzConfig
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.database.Database
import xtdb.database.DatabaseName
import xtdb.error.Incorrect
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.requiringResolve
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID.randomUUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.extension

/**
 * A node that does nothing but ingest from external sources — it runs a [Database] per configured
 * external source, joins leader election, and runs the source only on the database for which it's
 * elected leader. There's no query/client surface: no pgwire, no Flight SQL — though it can serve
 * the healthz endpoints ([Config.healthz]) for metrics and liveness.
 *
 * Started either from YAML config (`xtdb.main ingest`, [readConfig]) with registered external sources,
 * or embedded in a custom JVM application that supplies its own indexer programmatically.
 * Because the configuration is handed in fresh on every boot, a programmatically-supplied external
 * source's indexer factory needn't be serialisable — it never travels as persisted secondary-database config.
 */
class IngestNode internal constructor(
    private val base: NodeBase,
    private val databases: Map<DatabaseName, Database>,
    private val rootJob: Job,
    private val healthzServer: AutoCloseable?,
) : AutoCloseable {
    private val closing = AtomicBoolean(false)

    /**
     * The running [Database] for [name], if configured — lets an embedder reach the live database,
     * e.g. to await indexing progress or flush a block.
     */
    fun database(name: DatabaseName): Database? = databases[name]

    override fun close() {
        if (closing.compareAndSet(false, true)) {
            // healthz first — it reads the databases we're about to free.
            healthzServer?.close()

            // One cancel stops every database's job tree (Phase 1 — the single thread-parking bridge,
            // at node shutdown); then free the databases (each owns its LogProcessor / external source)
            // before the base, which owns the allocator they're children of.
            runBlocking { rootJob.cancelAndJoin() }
            databases.closeAll()
            base.close()
        }
    }

    // healthz reports on a Database.Catalog; the ingest node deliberately has no DatabaseCatalog,
    // so it hands healthz a fixed, read-only view over the databases opened at boot.
    private class FixedDbCatalog(private val dbs: Map<DatabaseName, Database>) : Database.Catalog {
        override val databaseNames get() = dbs.keys
        override fun databaseOrNull(dbName: DatabaseName) = dbs[dbName]

        override fun attach(dbName: DatabaseName, config: Database.Config?): Unit =
            error("can't attach a database to an ingest node")

        override fun detach(dbName: DatabaseName): Unit =
            error("can't detach a database from an ingest node")
    }

    @Serializable
    class Config(
        var logClusters: Map<RemoteAlias, Remote.Factory<*>> = emptyMap(),
        var remotes: Map<RemoteAlias, Remote.Factory<*>> = emptyMap(),

        /**
         * The databases to ingest into, keyed by name. Each carries an `externalSource` whose indexer
         * may be a programmatically-supplied, non-serialisable factory.
         */
        var databases: Map<DatabaseName, Database.Config> = emptyMap(),

        val memoryCache: MemoryCache.Factory = MemoryCache.Factory(),
        var diskCache: DiskCache.Factory? = null,

        var healthz: HealthzConfig? = null,

        val indexer: IndexerConfig = IndexerConfig(),

        var nodeId: String = System.getenv("XTDB_NODE_ID") ?: randomUUID().toString().takeWhile { it != '-' },
    ) {
        var allocator: BufferAllocator? = null

        fun logClusters(clusters: Map<RemoteAlias, Remote.Factory<*>>) = apply { logClusters += clusters }
        fun logCluster(alias: RemoteAlias, cluster: Remote.Factory<*>) = apply { logClusters += alias to cluster }

        fun remotes(remotes: Map<RemoteAlias, Remote.Factory<*>>) = apply { this.remotes += remotes }
        fun remote(alias: RemoteAlias, remote: Remote.Factory<*>) = apply { this.remotes += alias to remote }

        fun databases(databases: Map<DatabaseName, Database.Config>) = apply { this.databases += databases }
        fun database(name: DatabaseName, config: Database.Config) = apply { this.databases += name to config }

        fun diskCache(diskCache: DiskCache.Factory?) = apply { this.diskCache = diskCache }

        fun healthz(healthz: HealthzConfig) = apply { this.healthz = healthz }

        @JvmSynthetic
        fun indexer(configure: IndexerConfig.() -> Unit) = apply { indexer.configure() }

        fun nodeId(nodeId: String) = apply { this.nodeId = nodeId }

        /**
         * The base components ([NodeBase]) are assembled from an [Xtdb.Config] with no client surface
         * (`server`, `flightSql`, `healthz` all null — healthz is served by the ingest node itself,
         * not through the base config). The ingest databases aren't expressed here — [open] attaches
         * them directly, bypassing the [xtdb.database.DatabaseCatalog] and its "xtdb" primary entirely.
         */
        private fun toBaseConfig(): Xtdb.Config =
            Xtdb.Config(
                server = null,
                flightSql = null,
                healthz = null,
                logClusters = logClusters,
                remotes = remotes,
                memoryCache = memoryCache,
                diskCache = diskCache,
                indexer = indexer,
                nodeId = nodeId,
            ).also { it.allocator = allocator }

        fun open(): IngestNode {
            // "xtdb" is the primary database name; an ingest node has no primary, and opening a db so
            // named with dbCatalog=null trips a LeaderLogProcessor invariant only once it's elected
            // leader. Reject it up front rather than letting it surface as an async assertion failure.
            if ("xtdb" in databases)
                throw Incorrect(
                    "'xtdb' is reserved for the primary database and can't be used as an ingest-node database name",
                    errorCode = "xtdb.ingest-node/reserved-db-name",
                    data = mapOf("db-name" to "xtdb"),
                )

            // Open each external-source database directly with no catalog (dbCatalog=null): an ext-src
            // db never calls back into a catalog, so the ingest node needs neither the DatabaseCatalog
            // nor its "xtdb" primary. Each db joins leader election and runs its source only when leader.
            return NodeBase.openBase(toBaseConfig()).closeOnCatch { base ->
                // The ingest node owns its databases' job tree directly (no DatabaseCatalog). A
                // SupervisorJob so one database's failure doesn't cancel its siblings; `close`
                // cancels it once to stop every database in one go.
                val rootJob = SupervisorJob()
                val rootScope = CoroutineScope(rootJob)
                val openedDbs = LinkedHashMap<DatabaseName, Database>()
                val healthzServer: AutoCloseable?
                try {
                    for ((dbName, dbConfig) in databases)
                        openedDbs[dbName] = Database.open(base, dbName, dbConfig, base.compactor, rootScope)

                    healthzServer = healthz?.let {
                        openHealthzServer.invoke(base, FixedDbCatalog(openedDbs), it) as AutoCloseable
                    }
                } catch (t: Throwable) {
                    // Cancel the job tree (Phase 1) before freeing the already-opened databases
                    // (Phase 2) — closing them while their coroutines still run would free
                    // allocator-backed state out from under live work.
                    runBlocking { rootJob.cancelAndJoin() }
                    openedDbs.values.closeAll()
                    throw t
                }
                IngestNode(base, openedDbs, rootJob, healthzServer)
            }
        }
    }

    companion object {
        private val openHealthzServer by lazy { requiringResolve("xtdb.healthz/open-server") }

        @JvmStatic
        fun readConfig(path: Path): Config {
            if (path.extension != "yaml") {
                throw IllegalArgumentException("Invalid config file type - must be '.yaml'")
            } else if (!path.toFile().exists()) {
                throw IllegalArgumentException("Provided config file does not exist")
            }

            return ingestNodeConfig(Files.readString(path))
        }

        @JvmStatic
        @JvmOverloads
        fun openIngestNode(config: Config = Config()) = config.open()

        /**
         * Opens an ingest node using a YAML configuration file - will throw an exception if the specified path
         * does not exist or is not a valid `.yaml` extension file.
         */
        @JvmStatic
        fun openIngestNode(path: Path) = readConfig(path).open()

        @JvmSynthetic
        fun openIngestNode(configure: Config.() -> Unit) = openIngestNode(Config().also(configure))
    }
}
