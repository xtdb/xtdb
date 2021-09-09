package xtdb.api.query.conversion

import xtdb.api.query.domain.RuleDefinition
import xtdb.api.query.domain.WhereClause
import xtdb.api.underware.pl
import xtdb.api.underware.prefix
import xtdb.api.underware.pv

fun RuleDefinition.toEdn() = body.clauses.map(WhereClause::toEdn).prefix(
    if (boundParameters.isEmpty()) {
        parameters
    }
    else {
        parameters.prefix(boundParameters.pv)

    }
        .prefix(name)
        .pl
).pv