package xtdb.database

import xtdb.NodeBase
import xtdb.compactor.Compactor
import xtdb.database.proto.DatabaseConfig
import xtdb.error.Conflict
import xtdb.error.Incorrect
import xtdb.error.NotFound
import xtdb.indexer.Indexer
import xtdb.table.DatabaseName
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.logger
import xtdb.util.warn
import java.util.concurrent.ConcurrentHashMap

private val LOG = DatabaseCatalog::class.logger

class DatabaseCatalog(
    private val base: NodeBase,
    private val indexer: Indexer,
    private val compactor: Compactor,
) : Database.Catalog, AutoCloseable {

    private val databases = ConcurrentHashMap<DatabaseName, Database>()
    private val dormantDatabases = ConcurrentHashMap<DatabaseName, DatabaseConfig>()

    override val databaseNames: Collection<DatabaseName> get() = databases.keys.toSet()

    override fun databaseOrNull(dbName: DatabaseName): Database? = databases[dbName]

    override val serialisedSecondaryDatabases: Map<DatabaseName, DatabaseConfig>
        get() {
            val active = this.filterNot { it.name == "xtdb" }
                .associate { db -> db.name to db.config.serializedConfig }
            return active + dormantDatabases
        }

    private val skipDbs: Set<String> get() = base.config.skipDbs

    override fun attach(dbName: DatabaseName, config: Database.Config?) {
        if (databases.containsKey(dbName) || dormantDatabases.containsKey(dbName))
            throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to dbName))

        val dbConfig = config ?: Database.Config()

        if (dbName in skipDbs) {
            LOG.warn { "Skipping database '$dbName' (XTDB_SKIP_DBS) — database is dormant. Remove from XTDB_SKIP_DBS and restart to re-enable, or DETACH DATABASE to remove permanently." }
            dormantDatabases[dbName] = dbConfig.serializedConfig
            return
        }

        val readOnlyConfig = if (base.config.readOnlyDatabases) dbConfig.mode(Database.Mode.READ_ONLY) else dbConfig

        val db = try {
            Database.open(base, dbName, readOnlyConfig, indexer, compactor, this.takeIf { dbName == "xtdb" })
        } catch (t: Throwable) {
            LOG.debug { "Failed to open database: db-name=$dbName, exception=${t.javaClass}, message=${t.message}" }
            t.cause?.let { LOG.debug { "Cause: class=${it.javaClass}, message=${it.message}" } }
            if (t is IllegalStateException) throw t
            throw Incorrect("Failed to open database", "xtdb.db-catalog/invalid-db-config", mapOf("db-name" to dbName), t)
        }

        db.closeOnCatch {
            databases[dbName] = db
        }
    }

    override fun detach(dbName: DatabaseName) {
        if (dbName == "xtdb")
            throw Incorrect("Cannot detach the primary 'xtdb' database", "xtdb/cannot-detach-primary", mapOf("db-name" to dbName))

        if (dormantDatabases.remove(dbName) != null) return

        val db = databases.remove(dbName)
            ?: throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to dbName))

        db.close()
    }

    override fun close() {
        databases.values.closeAll()
        indexer.close()
    }

    companion object {
        @JvmStatic
        fun open(
            base: NodeBase,
        ): DatabaseCatalog {
            val indexer = base.indexerFactory.create(base)
            val catalog = DatabaseCatalog(base, indexer, base.compactor)

            catalog.closeOnCatch {
                val conf = base.config
                val xtdbDbConfig = Database.Config()
                    .log(conf.log)
                    .storage(conf.storage)
                    .let { if (conf.readOnlyDatabases) it.mode(Database.Mode.READ_ONLY) else it }

                catalog.attach("xtdb", xtdbDbConfig)

                val secondaryDbs = catalog.primary.blockCatalog.secondaryDatabases
                for ((dbName, dbProtoConfig) in secondaryDbs) {
                    if (dbName == "xtdb") continue
                    val dbConfig = Database.Config.fromProto(dbProtoConfig)
                    catalog.attach(dbName, dbConfig)
                }
            }

            return catalog
        }
    }
}
