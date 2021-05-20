package crux.api.query.conversion

import crux.api.query.domain.BindClause
import crux.api.underware.clj
import crux.api.underware.pv

private val COL = "...".clj

fun BindClause.toEdn() = when (this) {
    is BindClause.SimpleBind -> symbol
    is BindClause.CollectionBind -> listOf(symbol, COL).pv
}