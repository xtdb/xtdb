package crux.api.query.context

import crux.api.query.domain.Query
import crux.api.query.domain.QuerySection
import crux.api.query.domain.QuerySection.LimitSection
import crux.api.query.domain.QuerySection.OffsetSection
import crux.api.underware.BuilderContext
import javax.naming.OperationNotSupportedException

class QueryContext private constructor(): BuilderContext<Query> {
    companion object {
        fun build(block: QueryContext.() -> Unit) = QueryContext().also(block).build()
    }

    private val sections = mutableListOf<QuerySection>()

    private fun add(section: QuerySection) {
        sections.add(section)
    }

    operator fun QuerySection.unaryPlus() = add(this)
    fun find(block: FindContext.() -> Unit) = +FindContext.build(block)
    fun where(block: WhereContext.() -> Unit) = +WhereContext.build(block)
    fun order(block: OrderContext.() -> Unit) = +OrderContext.build(block)
    fun rules(block: RulesContext.() -> Unit) = +RulesContext.build(block)
    fun bind(block: BindContext.() -> Unit) = +BindContext.build(block)

    var offset: Int
        get() = throw OperationNotSupportedException()
        set(value) {
            +OffsetSection(value)
        }

    var limit: Int
        get() = throw OperationNotSupportedException()
        set(value) {
            +LimitSection(value)
        }

    override fun build() = Query(sections)
}
