package xtdb.query

import xtdb.database.Database

interface IQuerySource {
    fun prepareQuery(query: Any, db: Database?, opts: Any?): PreparedQuery
}