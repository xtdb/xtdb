package crux.api.query.conversion

import crux.api.query.domain.WhereClause
import crux.api.underware.pl
import crux.api.underware.prefix
import crux.api.underware.pv

fun WhereClause.toEdn(): Any = when (this) {
    is WhereClause.HasKey -> listOf(symbol, key).pv
    is WhereClause.HasKeyEqualTo -> listOf(document, key, value).pv
    is WhereClause.Join -> body.clauses.map(WhereClause::toEdn).prefix(type.symbol).pl
    is WhereClause.Predicate -> listOf(
        listOf(type.symbol, i, j).pl
    ).pv
    is WhereClause.RuleInvocation -> parameters.prefix(name).pl
}