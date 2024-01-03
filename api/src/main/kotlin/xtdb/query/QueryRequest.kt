package xtdb.query

data class QueryRequest(
    @get:JvmName("query") val query: Query,
    @get:JvmName("queryOpts") val queryOpts: QueryOpts
)
