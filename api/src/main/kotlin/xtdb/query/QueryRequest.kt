package xtdb.query

data class QueryRequest(
    @JvmField val query: Query,
    @JvmField val queryOpts: QueryOptions
)
