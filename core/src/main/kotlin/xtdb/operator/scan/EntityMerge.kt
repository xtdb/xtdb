package xtdb.operator.scan

import xtdb.trie.EventRowPointer

internal class LeafPointer(val evPtr: EventRowPointer, val relIdx: Int)

/** One entity's events, newest system-time first. */
internal interface EntityEvents {
    fun nextEvent(): LeafPointer?
}

/**
 * A binary min-heap of page pointers in [EventRowPointer.compare] order, so its top is the next event to serve.
 * A caller advances the top pointer in place, then calls [reseatTop].
 */
internal class LeafHeap(pointers: List<LeafPointer>) {
    private val heap = pointers.filter { it.evPtr.isValid() }.toTypedArray()
    private var size = heap.size

    init {
        for (i in size / 2 - 1 downTo 0) siftDown(i)
    }

    fun top(): LeafPointer? = if (size == 0) null else heap[0]

    fun reseatTop() {
        if (!heap[0].evPtr.isValid()) heap[0] = heap[--size]
        if (size > 0) siftDown(0)
    }

    private fun siftDown(from: Int) {
        val node = heap[from]
        var i = from

        while (true) {
            var child = 2 * i + 1
            if (child >= size) break
            if (child + 1 < size && precedes(heap[child + 1], heap[child])) child++
            if (!precedes(heap[child], node)) break

            heap[i] = heap[child]
            i = child
        }

        heap[i] = node
    }

    private fun precedes(l: LeafPointer, r: LeafPointer) = EventRowPointer.compare(l.evPtr, r.evPtr) < 0
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
