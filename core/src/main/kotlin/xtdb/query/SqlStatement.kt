package xtdb.query

import xtdb.table.TableRef

/**
 * A parsed transactional SQL statement, ready to apply.
 *
 * The [DmlQuery] variants run a planned query; GRANT/REVOKE aren't DML and carry no query, so they
 * sit alongside rather than carrying a nulled-out one.
 */
sealed interface SqlStatement {

    sealed interface DmlQuery : SqlStatement {
        val query: PreparedQuery
    }

    data class Put(override val query: PreparedQuery, val table: TableRef) : DmlQuery
    data class Patch(override val query: PreparedQuery, val table: TableRef) : DmlQuery
    data class Delete(override val query: PreparedQuery, val table: TableRef) : DmlQuery
    data class Erase(override val query: PreparedQuery, val table: TableRef) : DmlQuery
    data class Assert(override val query: PreparedQuery, val message: String?) : DmlQuery

    data class GrantRole(val user: String, val role: String) : SqlStatement
    data class RevokeRole(val user: String, val role: String) : SqlStatement
}
