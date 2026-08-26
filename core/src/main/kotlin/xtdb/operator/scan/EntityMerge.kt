package xtdb.operator.scan

import xtdb.trie.EventRowPointer
import java.util.Comparator.comparing
import java.util.PriorityQueue

internal class LeafPointer(val evPtr: EventRowPointer, val relIdx: Int)

/** One entity's events, newest system-time first. */
internal interface EntityEvents {
    fun nextEvent(): LeafPointer?
}

/**
 * Merges one merge task's pages: an outer pass over entities in iid order, and within each entity
 * an inner pass over its events in reverse system-time order.
 */
internal class EntityMerge(pointers: List<LeafPointer>) : EntityEvents {
    // outer: one entry per page, ordered by the iid that page is currently on.
    // inner: the pages sitting on the entity being resolved, newest system-time first.
    private val iidQueue = PriorityQueue<LeafPointer>(comparing({ it.evPtr }, EventRowPointer.iidComparator()))
    private val eventQueue = PriorityQueue<LeafPointer>(comparing({ it.evPtr }, EventRowPointer.systemFromComparator()))

    init {
        iidQueue.addAll(pointers)
    }

    private var iidHigh = 0L
    private var iidLow = 0L

    // advanced on the next `nextEvent` rather than eagerly, so the caller can still read the row it's on.
    private var current: LeafPointer? = null

    /** Advances to the next entity, returning false once the task's pages are exhausted. */
    fun nextEntity(): Boolean {
        // a resolver that stopped early leaves its entity's pointers queued, and `nextEvent` polls
        // without an iid check, so they'd be served as this entity's events
        skipEntity()

        val firstOfIid = iidQueue.poll() ?: return false

        iidHigh = firstOfIid.evPtr.iidHigh
        iidLow = firstOfIid.evPtr.iidLow

        eventQueue.add(firstOfIid)
        while (iidQueue.peek()?.evPtr?.sameIidAs(iidHigh, iidLow) == true)
            eventQueue.add(iidQueue.poll())

        return true
    }

    override fun nextEvent(): LeafPointer? {
        current?.let { leafPtr ->
            current = null
            leafPtr.evPtr.nextIndex()
            requeue(leafPtr)
        }

        return eventQueue.poll()?.also { current = it }
    }

    private fun skipEntity() {
        current?.let { leafPtr ->
            current = null
            leafPtr.evPtr.skipToNextIid()
            requeue(leafPtr)
        }

        while (true) {
            val leafPtr = eventQueue.poll() ?: break
            leafPtr.evPtr.skipToNextIid()
            requeue(leafPtr)
        }
    }

    private fun requeue(leafPtr: LeafPointer) {
        val evPtr = leafPtr.evPtr
        if (evPtr.isValid())
            (if (evPtr.sameIidAs(iidHigh, iidLow)) eventQueue else iidQueue).add(leafPtr)
    }
}
