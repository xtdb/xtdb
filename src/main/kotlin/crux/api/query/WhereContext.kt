package crux.api.query

import clojure.lang.Symbol
import crux.api.kw
import crux.api.pl
import crux.api.pv

class WhereContext {
    private val clauses = mutableListOf<Any>()

    infix fun Symbol.has(key: String) {
        clauses.add(
            listOf(this, key.kw).pv
        )
    }

    fun build() = clauses.pv
}