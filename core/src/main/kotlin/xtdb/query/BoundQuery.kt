package xtdb.query

import org.apache.arrow.vector.types.pojo.Field
import xtdb.ICursor

interface BoundQuery : AutoCloseable {

    val columnFields: List<Field>

    fun openCursor(): ICursor<*>

    /**
     * optional - if you close this BoundQuery it'll close any closed-over args relation
     */
    override fun close()
}