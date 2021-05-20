package crux.api.query.domain

import crux.api.query.context.QueryContext

data class LimitSection(val limit: Int): QuerySection {
    override val key = QueryContext.LIMIT
    override fun toEdn() = limit
}