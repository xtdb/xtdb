package crux.api.query.domain

import clojure.lang.Keyword
import crux.api.underware.pam

data class Query(val sections: List<QuerySection>) {
    fun toEdn() = sections.map {
        it.key to it.toEdn()
    }.toMap().pam
}

interface QuerySection {
    val key: Keyword
    fun toEdn(): Any
}

