package xtdb.segment

class MergeTask(val pages: List<Segment.Page<*>>, val path: ByteArray, private val lastUsedPages: List<Segment.Page<*>>) {
    /**
     * Releases the pages no later task uses — call once the relations loaded for this task are finished with.
     * Tasks MUST be consumed in plan order.
     */
    fun release() = lastUsedPages.forEach { it.release() }
}
