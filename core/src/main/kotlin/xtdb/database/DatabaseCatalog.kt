package xtdb.database

import clojure.lang.ILookup
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import java.time.Duration

private suspend fun DatabaseCatalog.syncAll0() = coroutineScope {
    databaseNames
        .mapNotNull { databaseOrNull(it) }
        .map { db ->
            launch {
                db.logProcessor.let { it.awaitAsync(it.latestSubmittedMsgId) }.await()
            }
        }
        .joinAll()
}

interface DatabaseCatalog : AutoCloseable, ILookup {
    val databaseNames: Collection<DatabaseName>

    fun databaseOrNull(dbName: DatabaseName): Database?

    operator fun get(dbName: DatabaseName) = databaseOrNull(dbName)

    override fun valAt(key: Any?) = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = databaseOrNull(key as DatabaseName) ?: notFound

    val primary: Database get() = this["xtdb"]!!

    fun createDatabase(dbName: DatabaseName): Database

    fun syncAll() = runBlocking { syncAll0() }
    fun syncAll(timeout: Duration) = runBlocking { withTimeout(timeout) { syncAll0() } }
}