package crux.api.query.offset

import crux.api.query.QueryContext
import crux.api.query.QuerySection

data class OffsetSection(val offset: Int): QuerySection {
    override val key = QueryContext.OFFSET
    override fun toEdn() = offset
}