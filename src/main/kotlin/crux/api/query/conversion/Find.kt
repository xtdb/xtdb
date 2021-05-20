package crux.api.query.conversion

import crux.api.query.domain.FindClause
import crux.api.underware.pl

fun FindClause.toEdn() = when (this) {
    is FindClause.SimpleFind -> symbol
    is FindClause.Aggregate -> listOf(type.symbol, symbol).pl
    is FindClause.AggregateWithNumber -> listOf(type, n, symbol).pl
}