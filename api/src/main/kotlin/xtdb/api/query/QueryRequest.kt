package xtdb.api.query

import kotlinx.serialization.Serializable

/**
 * @suppress
 */
@Serializable
data class QueryRequest(
    @JvmField val query: Query,
    @JvmField val queryOpts: QueryOptions
)
