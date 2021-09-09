package xtdb.api.query.conversion

import xtdb.api.query.domain.WhereClause
import xtdb.api.underware.pl
import xtdb.api.underware.prefix
import xtdb.api.underware.pv

fun WhereClause.toEdn(): Any = when (this) {
    is WhereClause.HasKey -> listOf(symbol, key).pv
    is WhereClause.HasKeyEqualTo -> listOf(document, key, value).pv
    is WhereClause.Join -> body.clauses.map(WhereClause::toEdn).prefix(type.symbol).pl
    is WhereClause.Predicate -> listOf(
        listOf(type.symbol, i, j).pl
    ).pv
    is WhereClause.RuleInvocation -> parameters.prefix(name).pl
    is WhereClause.SetToFunction -> listOf(
        listOf(type.symbol, i, j).pl,
        target
    ).pv
}