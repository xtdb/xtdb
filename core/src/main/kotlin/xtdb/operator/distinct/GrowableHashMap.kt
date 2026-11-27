package xtdb.operator.distinct

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.error.Unsupported
import xtdb.arrow.IntVector
import xtdb.util.closeOnCatch
import java.util.function.IntUnaryOperator

private const val INITIAL_CAPACITY = 16
private const val LOAD_FACTOR = 0.6

// ExtensibleBuffer.getInt computes the byte offset in Int, so an IntVector past 2^29 slots wraps.
private const val MAX_CAPACITY = 1 shl 29

internal class GrowableHashMap(
    private val al: BufferAllocator,
    private val maxCapacity: Int = MAX_CAPACITY,
) : AutoCloseable {

    private class Slots private constructor(private val rowIdxs: IntVector, private val hashes: IntVector) : AutoCloseable {
        val capacity = rowIdxs.valueCount
        private val mask = capacity - 1

        fun homeSlot(hash: Int) = hash and mask
        fun nextSlot(slot: Int) = (slot + 1) and mask

        // -1 for an empty slot: slots hold rowIdx + 1, so a freshly zeroed vector reads as empty.
        fun rowIdx(slot: Int) = rowIdxs.getInt(slot) - 1
        fun hash(slot: Int) = hashes.getInt(slot)

        fun set(slot: Int, hash: Int, rowIdx: Int) {
            rowIdxs[slot] = rowIdx + 1
            hashes[slot] = hash
        }

        fun add(hash: Int, rowIdx: Int) {
            var slot = homeSlot(hash)
            while (rowIdx(slot) >= 0) slot = nextSlot(slot)
            set(slot, hash, rowIdx)
        }

        override fun close() {
            rowIdxs.close()
            hashes.close()
        }

        companion object {
            fun open(al: BufferAllocator, capacity: Int) =
                IntVector.open(al, false, capacity).closeOnCatch { rowIdxs ->
                    IntVector.open(al, false, capacity).closeOnCatch { hashes -> Slots(rowIdxs, hashes) }
                }
        }
    }

    private var slots = Slots.open(al, INITIAL_CAPACITY)
    private var rowCount = 0

    fun findOrInsert(hash: Int, newRowIdx: Int, cmp: IntUnaryOperator, insert: Runnable): Int {
        var slot = slots.homeSlot(hash)

        while (true) {
            val rowIdx = slots.rowIdx(slot)
            if (rowIdx < 0) break
            if (slots.hash(slot) == hash && cmp.applyAsInt(rowIdx) == 1) return rowIdx
            slot = slots.nextSlot(slot)
        }

        insert.run()
        slots.set(slot, hash, newRowIdx)
        if (++rowCount > slots.capacity * LOAD_FACTOR) grow()
        return newRowIdx
    }

    private fun grow() {
        val oldSlots = slots

        if (oldSlots.capacity >= maxCapacity)
            throw Unsupported(
                "Too many distinct rows to deduplicate in one DISTINCT, GROUP BY, window or distinct aggregate: $rowCount",
                "xtdb/distinct-map-full",
                mapOf("row-count" to rowCount),
            )

        slots = Slots.open(al, oldSlots.capacity * 2).closeOnCatch { newSlots ->
            for (slot in 0 until oldSlots.capacity) {
                val rowIdx = oldSlots.rowIdx(slot)
                if (rowIdx >= 0) newSlots.add(oldSlots.hash(slot), rowIdx)
            }
            newSlots
        }

        oldSlots.close()
    }

    override fun close() = slots.close()
}
