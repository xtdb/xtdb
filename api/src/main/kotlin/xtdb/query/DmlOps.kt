package xtdb.query

import xtdb.query.Query.UnifyClause
import xtdb.query.TemporalFilter.TemporalExtents

sealed interface DmlOps {
    data class Insert(
        @JvmField val table: String,
        @JvmField val query: Query
    ) : DmlOps

    data class Update(
        @JvmField val table: String,
        @JvmField val setSpecs: List<Binding>,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null
    ) : DmlOps {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>) = copy(unifyClauses = unifyClauses)
    }

    data class Delete(
        @JvmField val table: String,
        @JvmField val forValidTime: TemporalExtents? = null,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause?>? = null
    ) : DmlOps {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause?>?) = copy(unifyClauses = unifyClauses)
    }

    data class Erase(
        @JvmField val table: String,
        @JvmField val bindSpecs: List<Binding>? = null,
        @JvmField val unifyClauses: List<UnifyClause>? = null
    ) : DmlOps {

        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = Erase(table, bindSpecs, unifyClauses)
    }

    class AssertExists(@JvmField val query: Query) : DmlOps
    class AssertNotExists(@JvmField val query: Query) : DmlOps

    companion object {
        @JvmStatic
        fun insert(table: String, query: Query): Insert = Insert(table, query)

        @JvmStatic
        fun update(table: String, setSpecs: List<Binding>) = Update(table, setSpecs)

        @JvmStatic
        fun delete(table: String): Delete = Delete(table)

        @JvmStatic
        fun erase(table: String): Erase = Erase(table)

        @JvmStatic
        fun assertExists(query: Query) = AssertExists(query)

        @JvmStatic
        fun assertNotExists(query: Query) = AssertNotExists(query)
    }
}
