package xtdb.api.query.conversion

import xtdb.api.query.domain.OrderClause
import xtdb.api.underware.pv

fun OrderClause.toEdn() = listOf(symbol, direction.keyword).pv