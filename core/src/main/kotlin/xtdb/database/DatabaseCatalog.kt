package xtdb.database

import clojure.lang.ILookup
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import java.time.Duration

private suspend fun DatabaseCatalog.syncAll0() = coroutineScope {
    databaseNames
        .flatMap { databaseOrNull(it).orEmpty() }
        .map { db ->
            async {
                db.logProcessor.let { it.awaitAsync(it.latestSubmittedMsgId) }.await()
            }
        }
        .awaitAll()
}

interface DatabaseCatalog : AutoCloseable, ILookup {
    val databaseNames: Collection<DatabaseName>

    fun databaseOrNull(dbName: DatabaseName): List<Database>?

    operator fun get(dbName: DatabaseName) = databaseOrNull(dbName)

    override fun valAt(key: Any?) = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = databaseOrNull(key as DatabaseName) ?: notFound

    val primary: Database get() = this["xtdb"]!![0]

    fun createDatabase(dbName: DatabaseName): List<Database>

    fun syncAll() = runBlocking { syncAll0() }
    fun syncAll(timeout: Duration) = runBlocking { withTimeout(timeout) { syncAll0() } }
}