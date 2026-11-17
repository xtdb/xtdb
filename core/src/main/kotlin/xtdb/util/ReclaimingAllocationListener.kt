package xtdb.util

import org.apache.arrow.memory.AllocationListener
import org.apache.arrow.memory.AllocationOutcome
import org.apache.arrow.memory.BufferAllocator
import java.util.concurrent.CopyOnWriteArrayList

/**
 * When an allocation under an allocator listened to by this fails, asks each registered [Reclaimer] in turn to free memory,
 * and has Arrow retry the allocation once one has.
 *
 * Child allocators inherit their parent's listener, so one of these on a root covers every allocation under it.
 */
class ReclaimingAllocationListener(private val inner: AllocationListener = AllocationListener.NOOP) : AllocationListener {

    fun interface Reclaimer {
        /**
         * Called on the thread whose allocation failed.
         *
         * @return true if it freed any memory
         */
        fun reclaim(bytes: Long): Boolean
    }

    private val reclaimers = CopyOnWriteArrayList<Reclaimer>()

    /**
     * @return closing it unregisters [reclaimer]
     */
    fun register(reclaimer: Reclaimer): AutoCloseable {
        reclaimers += reclaimer
        return AutoCloseable { reclaimers -= reclaimer }
    }

    override fun onFailedAllocation(size: Long, outcome: AllocationOutcome): Boolean =
        reclaimers.any { it.reclaim(size) } || inner.onFailedAllocation(size, outcome)

    // `by inner` would skip these: Kotlin doesn't delegate a Java interface's default methods
    override fun onPreAllocation(size: Long) = inner.onPreAllocation(size)
    override fun onAllocation(size: Long) = inner.onAllocation(size)
    override fun onRelease(size: Long) = inner.onRelease(size)
    override fun onChildAdded(parentAllocator: BufferAllocator, childAllocator: BufferAllocator) = inner.onChildAdded(parentAllocator, childAllocator)
    override fun onChildRemoved(parentAllocator: BufferAllocator, childAllocator: BufferAllocator) = inner.onChildRemoved(parentAllocator, childAllocator)
}
