package xtdb.operator.scan

import xtdb.trie.LeafHeap
import xtdb.trie.LeafPointer

/** One entity's events, newest system-time first. */
internal interface EntityEvents {
    fun nextEvent(): LeafPointer?
}

/**
 * Merges one merge task's pages: an outer pass over entities in iid order, and within each entity
 * an inner pass over its events in reverse system-time order.
 */
internal class EntityMerge(pointers: List<LeafPointer>) : EntityEvents {
    private val heap = LeafHeap(pointers)

    private var onEntity = false
    private var iidHigh = 0L
    private var iidLow = 0L

    // advanced on the next `nextEvent` rather than eagerly, so the caller can still read the row it's on.
    private var current: LeafPointer? = null

    private fun topOnEntity() = heap.top()?.takeIf { it.evPtr.sameIidAs(iidHigh, iidLow) }

    /** Advances to the next entity, returning false once the task's pages are exhausted. */
    fun nextEntity(): Boolean {
        if (onEntity) {
            current = null

            while (true) {
                val leafPtr = topOnEntity() ?: break
                leafPtr.evPtr.skipToNextIid()
                heap.reseatTop()
            }
        }

        val top = heap.top()
        onEntity = top != null
        if (top == null) return false

        iidHigh = top.evPtr.iidHigh
        iidLow = top.evPtr.iidLow
        return true
    }

    override fun nextEvent(): LeafPointer? {
        if (!onEntity) return null

        current?.let { leafPtr ->
            current = null
            leafPtr.evPtr.nextIndex()
            heap.reseatTop()
        }

        return topOnEntity()?.also { current = it }
    }
}
