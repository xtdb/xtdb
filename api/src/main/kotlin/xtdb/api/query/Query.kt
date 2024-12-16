package xtdb.api.query

import kotlinx.serialization.Serializable

sealed interface Query

@Serializable
data class SqlQuery(@JvmField val sql: String) : Query

sealed interface XtqlQuery : Query {

    sealed interface QueryTail
    sealed interface UnifyClause
}
