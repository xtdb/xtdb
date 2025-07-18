package xtdb.query

import xtdb.database.Database
import xtdb.indexer.Snapshot

interface IQuerySource {
    fun prepareQuery(query: Any, db: Database, wmSrc: Snapshot.Source, opts: Any?): PreparedQuery
}