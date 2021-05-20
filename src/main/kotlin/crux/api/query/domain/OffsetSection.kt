package crux.api.query.domain

import crux.api.query.context.QueryContext

data class OffsetSection(val offset: Int): QuerySection {
    override val key = QueryContext.OFFSET
    override fun toEdn() = offset
}