package xtdb.api.query.conversion

import xtdb.api.query.domain.BindClause
import xtdb.api.underware.clj
import xtdb.api.underware.pv

private val COL = "...".clj

fun BindClause.toEdn() = when (this) {
    is BindClause.SimpleBind -> symbol
    is BindClause.CollectionBind -> listOf(symbol, COL).pv
}