package crux.api.query

import clojure.lang.Keyword
import crux.api.ICruxDatasource
import crux.api.kw
import crux.api.pam

class QueryContext {
    companion object {
        val FIND = "find".kw
        val WHERE = "where".kw
        val IN = "in".kw
        val ORDER = "order-by".kw
        val OFFSET = "offset".kw
        val LIMIT = "limit".kw
        val RULES = "rules".kw
    }

    private val map = mutableMapOf<Keyword, Any>()

    fun find(block: FindContext.() -> Unit) {
        //TODO: Move to companion
        map[FIND] = FindContext().also(block).build()
    }

    fun where(block: WhereContext.() -> Unit) {
        map[WHERE] = WhereContext.build(block)
    }

    fun order(block: OrderContext.() -> Unit) {
        map[ORDER] = OrderContext.build(block)
    }

    fun build() = map.pam
}

fun ICruxDatasource.q(block: QueryContext.() -> Unit): MutableCollection<MutableList<*>> = query(QueryContext().also(block).build())