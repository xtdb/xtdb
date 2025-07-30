package xtdb.database

import clojure.lang.ILookup
import kotlinx.serialization.Serializable
import xtdb.api.log.Log
import xtdb.api.storage.Storage

interface DatabaseCatalog : AutoCloseable, ILookup {
    val databaseNames: Collection<DatabaseName>

    fun databaseOrNull(dbName: DatabaseName): List<Database>?

    operator fun get(dbName: DatabaseName) = databaseOrNull(dbName)

    override fun valAt(key: Any?) = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = databaseOrNull(key as DatabaseName) ?: notFound
    
    val primary: Database get() = this["xtdb"]!![0]

    fun createDatabase(dbName: DatabaseName): List<Database>
}