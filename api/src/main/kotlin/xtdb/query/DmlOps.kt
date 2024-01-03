package xtdb.query

import xtdb.query.Query.UnifyClause
import xtdb.query.TemporalFilter.TemporalExtents

interface DmlOps {
    data class Insert(
        @get:JvmName("table") val table: String,
        @get:JvmName("query") val query: Query
    ) : DmlOps

    data class Update(
        @get:JvmName("table") val table: String,
        @get:JvmName("setSpecs") val setSpecs: List<Binding>,
        @get:JvmName("forValidTime") val forValidTime: TemporalExtents? = null,
        @get:JvmName("bindSpecs") val bindSpecs: List<Binding>? = null,
        @get:JvmName("unifyClauses") val unifyClauses: List<UnifyClause>? = null
    ) : DmlOps {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>) = copy(unifyClauses = QueryUtil.unmodifiableList(unifyClauses))
    }

    data class Delete(
        @get:JvmName("table") val table: String,
        @get:JvmName("forValidTime") val forValidTime: TemporalExtents? = null,
        @get:JvmName("bindSpecs") val bindSpecs: List<Binding>? = null,
        @get:JvmName("unifyClauses") val unifyClauses: List<UnifyClause?>? = null
    ) : DmlOps {

        fun forValidTime(forValidTime: TemporalExtents) = copy(forValidTime = forValidTime)
        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause?>?) = copy(unifyClauses = unifyClauses)
    }

    data class Erase(
        @get:JvmName("table") val table: String,
        @get:JvmName("bindSpecs") val bindSpecs: List<Binding>? = null,
        @get:JvmName("unifyClauses") val unifyClauses: List<UnifyClause>? = null
    ) : DmlOps {

        fun binding(bindSpecs: List<Binding>?) = copy(bindSpecs = bindSpecs)
        fun unify(unifyClauses: List<UnifyClause>?) = Erase(table, bindSpecs, unifyClauses)
    }

    class AssertExists(
        @get:JvmName("query") val query: Query
    ) : DmlOps

    class AssertNotExists(
        @get:JvmName("query") val query: Query
    ) : DmlOps

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
