package xtdb.api.query.conversion

import xtdb.api.query.domain.FindClause
import xtdb.api.underware.pl
import xtdb.api.underware.sym

private val pull = "pull".sym

fun FindClause.toEdn() = when (this) {
    is FindClause.SimpleFind -> symbol
    is FindClause.Aggregate -> listOf(type.symbol, symbol).pl
    is FindClause.AggregateWithNumber -> listOf(type, n, symbol).pl
    is FindClause.Pull -> listOf(pull, symbol, spec.toEdn()).pl
}