package xtdb.query

import xtdb.database.Database
import xtdb.indexer.Watermark

interface IQuerySource {
    fun prepareQuery(query: Any, db: Database, wmSrc: Watermark.Source, opts: Any?): PreparedQuery
}