package xtdb.database

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.time.withTimeout
import xtdb.NodeBase
import xtdb.compactor.Compactor
import xtdb.database.proto.DatabaseConfig
import xtdb.diagnostics.TeardownStall
import xtdb.error.Conflict
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.error.NotFound
import xtdb.table.DatabaseName
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.warn
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

private val LOG = DatabaseCatalog::class.logger

// A cancellation-immune wait in some database's tree (xtdb#5711) would otherwise hang teardown
// forever. Generous vs a real teardown (seconds).
private val CLOSE_TIMEOUT: Duration = Duration.ofSeconds(60)

class DatabaseCatalog @JvmOverloads constructor(
    private val base: NodeBase,
    private val compactor: Compactor,
    closerDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : Database.Catalog, AutoCloseable {

    private val databases = ConcurrentHashMap<DatabaseName, Database>()
    private val dormantDatabases = ConcurrentHashMap<DatabaseName, DatabaseConfig>()

    // Parent of every database's job tree. A SupervisorJob so one database's failure is contained
    // here (on the common parent) rather than cancelling its siblings; node shutdown cancels this
    // once to stop every database's indexing/compaction in one go.
    private val dbJob = SupervisorJob()
    private val dbScope = CoroutineScope(dbJob)

    // Detaching databases tear down off the caller's thread on this scope, and stay in `databases`
    // until that completes — see #5613. Nested under `dbJob` so node shutdown's single cancel covers it.
    private val closerJob = SupervisorJob(dbJob)
    private val closerScope = CoroutineScope(closerJob + closerDispatcher)

    override val databaseNames: Collection<DatabaseName>
        get() = databases.entries.asSequence().filter { !it.value.isClosing }.map { it.key }.toSet()

    override fun databaseOrNull(dbName: DatabaseName): Database? =
        databases[dbName]?.takeUnless { it.isClosing }

    override val serialisedSecondaryDatabases: Map<DatabaseName, DatabaseConfig>
        get() {
            val active = this.filterNot { it.name == "xtdb" }
                .associate { db -> db.name to db.config.serializedConfig }
            return active + dormantDatabases
        }

    private val skipDbs: Set<String> get() = base.config.skipDbs

    override fun attach(dbName: DatabaseName, config: Database.Config?) {
        databases[dbName]?.let { existing ->
            if (existing.isClosing)
                throw Conflict(
                    "Database is still being detached — retry once the previous detach has completed",
                    "xtdb/db-being-detached",
                    mapOf("db-name" to dbName)
                )
            throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to dbName))
        }
        if (dormantDatabases.containsKey(dbName))
            throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to dbName))

        val dbConfig = config ?: Database.Config()

        if (dbName in skipDbs) {
            LOG.warn { "Skipping database '$dbName' (XTDB_SKIP_DBS) — database is dormant. Remove from XTDB_SKIP_DBS and restart to re-enable, or DETACH DATABASE to remove permanently." }
            dormantDatabases[dbName] = dbConfig.serializedConfig
            return
        }

        val readOnlyConfig = if (base.config.readOnlyDatabases) dbConfig.mode(Database.Mode.READ_ONLY) else dbConfig

        val db = try {
            Database.open(base, dbName, readOnlyConfig, compactor, dbScope, this.takeIf { dbName == "xtdb" })
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

        val db = databases[dbName]
            ?: throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to dbName))

        if (db.isClosing)
            throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to dbName))

        // Close off the persister's stack — see #5613. `cancelAndJoin` suspends rather than parking a
        // thread in `runBlocking`, so the detach can't deadlock against another thread-parking
        // teardown on a constrained dispatcher.
        db.isClosing = true
        closerScope.launch {
            // NonCancellable: once teardown starts it must run to completion. Node shutdown cancels
            // `dbJob` (this coroutine's ancestor); without the shield a detach caught mid-cancelAndJoin
            // would skip `db.close()` yet still remove the database from the map — leaking its state.
            withContext(NonCancellable) {
                try {
                    db.cancelAndJoin()
                    db.close()
                } catch (t: Throwable) {
                    LOG.error(t) { "Failed to close detaching database '$dbName'" }
                } finally {
                    databases.remove(dbName, db)
                }
            }
        }
    }

    override fun close() {
        val stalled = runBlocking {
            try {
                withTimeout(CLOSE_TIMEOUT) {
                    // Let in-flight detaches finish their own teardown before we cancel the tree.
                    closerJob.children.toList().forEach { it.join() }
                    dbJob.cancelAndJoin()
                }
                false
            } catch (_: TimeoutCancellationException) {
                true
            }
        }

        if (stalled) {
            // Can't free a wedged tree — closing its allocator under a live coroutine is a
            // use-after-free — so leak it and fail loud. Dump first; the fast return leaves the
            // watchdog nothing to catch.
            TeardownStall.onStall("DatabaseCatalog.close exceeded ${CLOSE_TIMEOUT.toSeconds()}s")
            throw Fault(
                "database catalog did not shut down within ${CLOSE_TIMEOUT.toSeconds()}s",
                "xtdb/db-close-timeout"
            )
        }

        // Phase 1 joined — no coroutine is still using this state.
        databases.values.closeAll()
    }

    companion object {
        @JvmStatic
        @JvmOverloads
        fun open(
            base: NodeBase,
            closerDispatcher: CoroutineDispatcher = Dispatchers.IO,
        ): DatabaseCatalog {
            val catalog = DatabaseCatalog(base, base.compactor, closerDispatcher)

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
