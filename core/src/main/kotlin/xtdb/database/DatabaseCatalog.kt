package xtdb.database

import clojure.lang.ILookup

interface DatabaseCatalog : AutoCloseable, ILookup {
    val databaseNames: Collection<DatabaseName>

    fun databaseOrNull(dbName: DatabaseName): List<Database>?

    operator fun get(dbName: DatabaseName) = databaseOrNull(dbName)

    override fun valAt(key: Any?) = valAt(key, null)
    override fun valAt(key: Any?, notFound: Any?) = databaseOrNull(key as DatabaseName) ?: notFound
    
    val primary: Database get() = this["xtdb"]!![0]

    // TODO will eventually take config
    fun createDatabase(dbName: DatabaseName): List<Database>

    fun dropDatabase(dbName: DatabaseName)
}