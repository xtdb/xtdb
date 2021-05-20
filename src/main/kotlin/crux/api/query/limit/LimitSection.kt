package crux.api.query.limit

import crux.api.query.QueryContext
import crux.api.query.QuerySection

data class LimitSection(val limit: Int): QuerySection {
    override val key = QueryContext.LIMIT
    override fun toEdn() = limit
}