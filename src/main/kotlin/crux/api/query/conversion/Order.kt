package crux.api.query.conversion

import crux.api.query.domain.OrderClause
import crux.api.underware.pv

fun OrderClause.toEdn() = listOf(symbol, direction.keyword).pv