package xtdb.query

import xtdb.database.Database

interface IQuerySource {
    fun prepareQuery(query: Any, dbs: Database.Catalog, opts: Any?): PreparedQuery
}