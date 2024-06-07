package xtdb.api.query

import kotlinx.serialization.Serializable

/**
 * @suppress
 */
@Serializable
data class QueryRequest(
    @JvmField val sql: String,
    @JvmField val queryOpts: QueryOptions? = null
)
