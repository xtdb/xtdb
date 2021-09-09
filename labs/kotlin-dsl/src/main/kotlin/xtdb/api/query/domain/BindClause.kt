package xtdb.api.query.domain

import clojure.lang.Symbol

sealed class BindClause {
    data class SimpleBind(val symbol: Symbol): BindClause()
    data class CollectionBind(val symbol: Symbol): BindClause()
}