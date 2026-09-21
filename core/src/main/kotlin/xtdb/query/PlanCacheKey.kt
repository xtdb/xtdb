package xtdb.query

import org.apache.arrow.vector.types.pojo.Field
import xtdb.database.DatabaseName

/**
 * Everything a plan can depend on, and so everything the plan cache keys on.
 *
 * [ast] is compared by reference: ANTLR parse trees don't override `equals`, so two separately-parsed
 * trees for the same SQL are two keys. `SqlParser`'s text-keyed tree cache is what makes repeated SQL
 * reach one tree, and an eviction there is a miss here too.
 *
 * [tableInfo] is the schema the plan was built against, so a plan never outlives the catalog it assumed.
 */
data class PlanCacheKey(
    val ast: Any,
    val defaultDb: DatabaseName?,
    val dbNames: List<DatabaseName>?,
    val txScoped: Boolean,
    val decorrelate: Boolean,
    val explain: Explain?,
    val argFields: List<Field>?,
    val tableInfo: Any?,
) {
    enum class Explain { PLAN, ANALYZE }
}
