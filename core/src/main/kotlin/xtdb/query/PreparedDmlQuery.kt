package xtdb.query

import xtdb.table.TableRef

class PreparedDmlQuery(
    private val inner: PreparedQuery,
    val kind: Kind,
) : PreparedQuery by inner {

    sealed interface Kind

    data class Put(val table: TableRef) : Kind
    data class Patch(val table: TableRef) : Kind
    data class Delete(val table: TableRef) : Kind
    data class Erase(val table: TableRef) : Kind
    data class Assert(val message: String?) : Kind
}
