package xtdb.query

import xtdb.indexer.Watermark

interface IQuerySource {
    fun prepareRaQuery(raQuery: Any, opts: Any?): PreparedQuery
    fun prepareRaQuery(raQuery: Any, wmSrc: Watermark.Source, opts: Any?): PreparedQuery

    fun planQuery(query: Any, opts: Any?): Any
    fun planQuery(query: Any, wmSrc: Watermark.Source, opts: Any?): Any
}