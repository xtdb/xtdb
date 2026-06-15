package xtdb.api

import org.apache.arrow.memory.BufferAllocator
import xtdb.NodeBase
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.database.Database
import xtdb.database.DatabaseName
import xtdb.error.Incorrect
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.safeMapValues
import java.util.UUID.randomUUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A node that does nothing but ingest from external sources — it runs a [Database] per configured
 * external source, joins leader election, and runs the source only on the database for which it's
 * elected leader. There's no query/client surface: no pgwire, no Flight SQL, no healthz.
 *
 * Intended for embedding in a custom JVM application that supplies its own indexer programmatically.
 * Because the configuration lives in code and is handed in fresh on every boot, an external source's
 * indexer factory needn't be serialisable — it never travels as persisted secondary-database config.
 */
class IngestNode internal constructor(
    private val base: NodeBase,
    private val databases: Map<DatabaseName, Database>,
) : AutoCloseable {
    private val closing = AtomicBoolean(false)

    override fun close() {
        if (closing.compareAndSet(false, true)) {
            // close the databases (each owns its own LogProcessor / external source) before the base,
            // which owns the allocator they're children of.
            databases.closeAll()
            base.close()
        }
    }

    class Config {
        var allocator: BufferAllocator? = null

        var logClusters: Map<RemoteAlias, Remote.Factory<*>> = emptyMap()
        var remotes: Map<RemoteAlias, Remote.Factory<*>> = emptyMap()

        /**
         * The databases to ingest into, keyed by name. Each carries an `externalSource` whose indexer
         * may be a programmatically-supplied, non-serialisable factory.
         */
        var databases: Map<DatabaseName, Database.Config> = emptyMap()

        val memoryCache: MemoryCache.Factory = MemoryCache.Factory()
        var diskCache: DiskCache.Factory? = null

        val indexer: IndexerConfig = IndexerConfig()

        var nodeId: String = System.getenv("XTDB_NODE_ID") ?: randomUUID().toString().takeWhile { it != '-' }

        fun logClusters(clusters: Map<RemoteAlias, Remote.Factory<*>>) = apply { logClusters += clusters }
        fun logCluster(alias: RemoteAlias, cluster: Remote.Factory<*>) = apply { logClusters += alias to cluster }

        fun remotes(remotes: Map<RemoteAlias, Remote.Factory<*>>) = apply { this.remotes += remotes }
        fun remote(alias: RemoteAlias, remote: Remote.Factory<*>) = apply { this.remotes += alias to remote }

        fun databases(databases: Map<DatabaseName, Database.Config>) = apply { this.databases += databases }
        fun database(name: DatabaseName, config: Database.Config) = apply { this.databases += name to config }

        fun diskCache(diskCache: DiskCache.Factory?) = apply { this.diskCache = diskCache }

        @JvmSynthetic
        fun indexer(configure: IndexerConfig.() -> Unit) = apply { indexer.configure() }

        fun nodeId(nodeId: String) = apply { this.nodeId = nodeId }

        /**
         * The base components ([NodeBase]) are assembled from an [Xtdb.Config] with no client surface
         * (`server`, `flightSql`, `healthz` all null). The ingest databases aren't expressed here —
         * [open] attaches them directly, bypassing the [xtdb.database.DatabaseCatalog] and its "xtdb"
         * primary entirely.
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
                val dbs = databases.safeMapValues { dbName, dbConfig ->
                    Database.open(base, dbName, dbConfig, base.compactor)
                }
                IngestNode(base, dbs)
            }
        }
    }

    companion object {
        @JvmStatic
        @JvmOverloads
        fun openIngestNode(config: Config = Config()) = config.open()

        @JvmSynthetic
        fun openIngestNode(configure: Config.() -> Unit) = openIngestNode(Config().also(configure))
    }
}
