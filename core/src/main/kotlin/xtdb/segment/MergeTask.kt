package xtdb.segment

import xtdb.util.closeAll

class MergeTask(val pages: List<Segment.Page<*>>, val path: ByteArray, private val lastUsedPages: List<Segment.Page<*>>) : AutoCloseable {
    /**
     * Closes only the pages no later task uses — call once the relations loaded for this task are finished with.
     * Tasks MUST be consumed in plan order.
     */
    override fun close() = lastUsedPages.closeAll()
}
