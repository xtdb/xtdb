package xtdb.database

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import xtdb.NodeBase
import xtdb.compactor.Compactor
import xtdb.database.proto.DatabaseConfig
import xtdb.diagnostics.TeardownStall
import xtdb.api.error.Conflict
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.api.error.NotFound
import xtdb.api.DatabaseName
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.warn
import java.util.concurrent.ConcurrentHashMap

private val LOG = DatabaseCatalog::class.logger

class DatabaseCatalog @JvmOverloads constructor(
    private val base: NodeBase,
    private val compactor: Compactor,
    closerDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : Database.Catalog, AutoCloseable {

    // A block records the whole secondary list and replaces the previous one, so an entry left out
    // of `serialisedSecondaryDatabases` while a block is cut is erased rather than skipped.
    private sealed interface Entry {
        val config: Database.Config

        class Open(val db: Database) : Entry {
            override val config get() = db.config
        }

        class Skipped(override val config: Database.Config) : Entry

        class Detaching(val db: Database) : Entry {
            override val config get() = db.config
        }
    }

    private val entries = ConcurrentHashMap<DatabaseName, Entry>()

    // Parent of every database's job tree. A SupervisorJob so one database's failure is contained
    // here (on the common parent) rather than cancelling its siblings; node shutdown cancels this
    // once to stop every database's indexing/compaction in one go.
    private val dbJob = SupervisorJob()
    private val dbScope = CoroutineScope(dbJob)

    // Detaching databases tear down off the caller's thread on this scope, and keep their entry
    // until that completes — see #5613. Nested under `dbJob` so node shutdown's single cancel covers it.
    private val closerJob = SupervisorJob(dbJob)
    private val closerScope = CoroutineScope(closerJob + closerDispatcher)

    override val databaseNames: Collection<DatabaseName>
        get() = entries.entries.asSequence().filter { it.value is Entry.Open }.map { it.key }.toSet()

    override val txScoped = false

    override fun databaseOrNull(dbName: DatabaseName): Database? = (entries[dbName] as? Entry.Open)?.db

    override val serialisedSecondaryDatabases: Map<DatabaseName, DatabaseConfig>
        get() = entries.entries
            .filter { it.key != "xtdb" && it.value !is Entry.Detaching }
            .associate { (dbName, entry) -> dbName to entry.config.serializedConfig }

    private val skipDbs: Set<String> get() = base.config.skipDbs

    override fun checkCanAttach(dbName: DatabaseName, config: Database.Config) {
        when (entries[dbName]) {
            is Entry.Detaching -> throw Conflict(
                "Database is still being detached — retry once the previous detach has completed",
                "xtdb/db-being-detached",
                mapOf("db-name" to dbName)
            )

            is Entry.Open, is Entry.Skipped ->
                throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to dbName))

            null -> {}
        }

        config.checkValid(dbName)
    }

    override fun checkCanDetach(dbName: DatabaseName) {
        if (dbName == "xtdb")
            throw Incorrect("Cannot detach the primary 'xtdb' database", "xtdb/cannot-detach-primary", mapOf("db-name" to dbName))

        when (entries[dbName]) {
            is Entry.Open, is Entry.Skipped -> {}

            is Entry.Detaching, null ->
                throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to dbName))
        }
    }

    override fun attach(dbName: DatabaseName, config: Database.Config?) {
        val dbConfig = config ?: Database.Config()
        checkCanAttach(dbName, dbConfig)

        if (dbName in skipDbs) {
            LOG.warn { "Skipping database '$dbName' (XTDB_SKIP_DBS) — database is dormant. Remove from XTDB_SKIP_DBS and restart to re-enable, or DETACH DATABASE to remove permanently." }
            entries[dbName] = Entry.Skipped(dbConfig)
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
            entries[dbName] = Entry.Open(db)
        }
    }

    override fun detach(dbName: DatabaseName) {
        checkCanDetach(dbName)

        fun noSuchDb(): Nothing =
            throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to dbName))

        val open = when (val entry = entries[dbName]) {
            is Entry.Skipped -> {
                if (!entries.remove(dbName, entry)) noSuchDb()
                return
            }

            is Entry.Open -> entry

            is Entry.Detaching, null -> noSuchDb()
        }

        // Close off the persister's stack — see #5613. `cancelAndJoin` suspends rather than parking a
        // thread in `runBlocking`, so the detach can't deadlock against another thread-parking
        // teardown on a constrained dispatcher.
        val detaching = Entry.Detaching(open.db)
        if (!entries.replace(dbName, open, detaching)) noSuchDb()

        closerScope.launch {
            // NonCancellable: once teardown starts it must run to completion. Node shutdown cancels
            // `dbJob` (this coroutine's ancestor); without the shield a detach caught mid-cancelAndJoin
            // would skip `db.close()` yet still drop the entry — leaking its state.
            withContext(NonCancellable) {
                try {
                    open.db.cancelAndJoin()
                    open.db.close()
                } catch (t: Throwable) {
                    LOG.error(t) { "Failed to close detaching database '$dbName'" }
                } finally {
                    entries.remove(dbName, detaching)
                }
            }
        }
    }

    override fun close() {
        val stalled = TeardownStall.runBounded("DatabaseCatalog.close") {
            // Let in-flight detaches finish their own teardown before we cancel the tree.
            closerJob.children.toList().forEach { it.join() }
            dbJob.cancelAndJoin()
        }

        if (stalled) {
            // Skip Phase 2: freeing an allocator while the wedged tree is still live is a
            // use-after-free. Leak it and fail loud (runBounded already dumped).
            throw Fault("database catalog did not shut down in time", "xtdb/db-close-timeout")
        }

        entries.values.mapNotNull {
            when (it) {
                is Entry.Open -> it.db
                is Entry.Detaching -> it.db
                is Entry.Skipped -> null
            }
        }.closeAll()
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

                val secondaryDbs = catalog.primary.tableCatalog.secondaryDatabases
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
