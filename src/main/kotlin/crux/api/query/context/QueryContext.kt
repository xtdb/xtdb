package crux.api.query.context

import crux.api.ICruxDatasource
import crux.api.query.domain.Query
import crux.api.query.domain.LimitSection
import crux.api.query.domain.OffsetSection
import crux.api.query.domain.QuerySection
import crux.api.underware.kw
import crux.api.underware.BuilderContext
import javax.naming.OperationNotSupportedException

class QueryContext private constructor(): BuilderContext<Query> {
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

    private val sections = mutableListOf<QuerySection>()

    fun find(block: FindContext.() -> Unit) {
        sections.add(FindContext.build(block))
    }

    fun where(block: WhereContext.() -> Unit) {
        sections.add(WhereContext.build(block))
    }

    fun order(block: OrderContext.() -> Unit) {
        sections.add(OrderContext.build(block))
    }

    fun rules(block: RulesContext.() -> Unit) {
        sections.add(RulesContext.build(block))
    }

    fun bind(block: BindContext.() -> Unit) {
        sections.add(BindContext.build(block))
    }

    var offset: Int
        get() = throw OperationNotSupportedException()
        set(value) {
            sections.add(OffsetSection(value))
        }

    var limit: Int
        get() = throw OperationNotSupportedException()
        set(value) {
            sections.add(LimitSection(value))
        }

    override fun build() = Query(sections)
}

fun ICruxDatasource.q(vararg params: Any, block: QueryContext.() -> Unit): MutableCollection<MutableList<*>> = query(QueryContext.build(block).toEdn(), *params)