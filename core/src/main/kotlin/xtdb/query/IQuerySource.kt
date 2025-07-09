package xtdb.query

import xtdb.indexer.Watermark

interface IQuerySource {
    fun prepareQuery(query: Any, opts: Any?): PreparedQuery
    fun prepareQuery(query: Any, wmSrc: Watermark.Source, opts: Any?): PreparedQuery
}