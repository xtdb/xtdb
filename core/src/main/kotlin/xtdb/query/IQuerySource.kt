package xtdb.query

import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.indexer.Snapshot

interface IQuerySource {

    interface QueryCatalog {
        val databaseNames: Collection<DatabaseName>
        fun databaseOrNull(dbName: DatabaseName): QueryDatabase?
    }

    interface QueryDatabase : Snapshot.Source {
        val storage: DatabaseStorage
        val queryState: DatabaseState
    }

    fun prepareQuery(query: Any, dbs: QueryCatalog, opts: Any?): PreparedQuery
}
