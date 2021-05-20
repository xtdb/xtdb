package crux.api.query.conversion

import crux.api.query.domain.FindClause
import crux.api.underware.pl
import crux.api.underware.sym

private val pull = "pull".sym

fun FindClause.toEdn() = when (this) {
    is FindClause.SimpleFind -> symbol
    is FindClause.Aggregate -> listOf(type.symbol, symbol).pl
    is FindClause.AggregateWithNumber -> listOf(type, n, symbol).pl
    is FindClause.Pull -> listOf(pull, symbol, spec.toEdn()).pl
}