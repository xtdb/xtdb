package crux.api.query

import clojure.lang.Keyword
import crux.api.ICruxDatasource
import crux.api.kw
import crux.api.pam
import jdk.dynalink.Operation
import javax.naming.OperationNotSupportedException

class QueryContext private constructor() {
    companion object {
        val FIND = "find".kw
        val WHERE = "where".kw
        val IN = "in".kw
        val ORDER = "order-by".kw
        val OFFSET = "offset".kw
        val LIMIT = "limit".kw
        val RULES = "rules".kw

        fun build(block: QueryContext.() -> Unit) = QueryContext().also(block).build()
    }

    private val map = mutableMapOf<Keyword, Any>()

    fun find(block: FindContext.() -> Unit) {
        map[FIND] = FindContext.build(block)
    }

    fun where(block: WhereContext.() -> Unit) {
        map[WHERE] = WhereContext.build(block)
    }

    fun order(block: OrderContext.() -> Unit) {
        map[ORDER] = OrderContext.build(block)
    }

    fun rules(block: RulesContext.() -> Unit) {
        map[RULES] = RulesContext.build(block)
    }

    var offset: Int
        get() = throw OperationNotSupportedException()
        set(value) {
            map[OFFSET] = value
        }

    var limit: Int
        get() = throw OperationNotSupportedException()
        set(value) {
            map[LIMIT] = value
        }

    private fun build() = map.pam
}

fun ICruxDatasource.q(block: QueryContext.() -> Unit): MutableCollection<MutableList<*>> = query(QueryContext.build(block))