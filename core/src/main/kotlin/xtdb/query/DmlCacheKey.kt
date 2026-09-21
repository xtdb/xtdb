package xtdb.query

import org.apache.arrow.vector.types.pojo.Field
import xtdb.database.DatabaseName

/**
 * Identifies the statement- and catalog-derived half of a DML statement's planning env — what [PlanCacheKey]
 * is to a query, for the eager reduction of DML to core ops.
 *
 * What it keys is arg-independent: the mutable counters a reduction needs are made fresh per execute, and the
 * argument *values* reach the visitor separately. Only the argument *types* are keyed on, in [argFields].
 *
 * [ast] is compared by reference, as in [PlanCacheKey] — and here that is load-bearing rather than only a
 * matter of hit rate. The cached value holds `dynamic-param-idxs`, an `IdentityHashMap` keyed on nodes of
 * this tree, so it is meaningful only for the very tree it was built from. Comparing ASTs structurally
 * would hand a reduction the parameter indices of a different parse.
 */
data class DmlCacheKey(
    val ast: Any,
    val defaultDb: DatabaseName?,
    val dbNames: List<DatabaseName>?,
    val txScoped: Boolean,
    val argFields: List<Field>?,
    val tableInfo: Any?,
)
