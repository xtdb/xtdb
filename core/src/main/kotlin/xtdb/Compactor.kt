@file:JvmName("Compactor")

package xtdb

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.RecencyGranularity.*
import xtdb.trie.HashTrie
import xtdb.trie.HashTrie.Companion.LEVEL_WIDTH
import xtdb.trie.ITrieWriter
import xtdb.vector.IVectorReader
import xtdb.vector.RelationReader
import java.time.Instant
import java.time.LocalTime.MIDNIGHT
import java.time.OffsetDateTime
import java.time.YearMonth
import java.time.ZoneOffset.UTC
import java.util.*
import java.util.stream.IntStream
import kotlin.math.ceil
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

internal fun Selection.partitionSlices(partIdxs: IntArray) =
    Array(LEVEL_WIDTH) { partition ->
        val cur = partIdxs[partition]
        val nxt = if (partition == partIdxs.lastIndex) size else partIdxs[partition + 1]

        if (cur == nxt) null else sliceArray(cur..<nxt)
    }

internal fun Selection.iidPartitions(iidReader: IVectorReader, level: Int): Array<Selection?> {
    val iidPtr = ArrowBufPointer()

    // for each partition, find the starting index in the selection
    val partIdxs = IntArray(LEVEL_WIDTH) { partition ->
        var left = 0
        var right = size
        var mid: Int
        while (left < right) {
            mid = (left + right) / 2

            val bucket = HashTrie.bucketFor(iidReader.getPointer(this[mid], iidPtr), level)

            if (bucket < partition) left = mid + 1 else right = mid
        }

        left
    }

    // slice the selection array for each partition
    return partitionSlices(partIdxs)
}

internal enum class RecencyGranularity {
    YEAR, QUARTER, MONTH
}

private typealias InstantMicros = Long
private typealias Selection = IntArray

private val YearMonth.startMicros get() = atDay(1).toEpochSecond(MIDNIGHT, UTC) * 1_000_000

internal fun InstantMicros.toDateTime() =
    OffsetDateTime.ofInstant(Instant.ofEpochSecond(this / 1_000_000, this % 1_000), UTC)

internal fun InstantMicros.recencyBucket(depth: RecencyGranularity): InstantMicros {
    // TODO will likely need some caching here.
    if (this == MAX_LONG) return MAX_LONG

    val ym = YearMonth.from(toDateTime())
    return when (depth) {
        MONTH -> ym.plusMonths(1)
        QUARTER -> ym.withMonth(ym.month.firstMonthOfQuarter().value).plusMonths(3)
        YEAR -> ym.plusYears(1).withMonth(1)
    }.startMicros
}

internal fun IntArray.recencyPartitions(
    recencies: IVectorReader,
    depth: RecencyGranularity,
): SortedMap<InstantMicros, Selection> {
    val res = sortedMapOf<InstantMicros, IntArrayList>()

    for (idx in this) {
        val recency = recencies.getLong(idx).recencyBucket(depth)
        res.computeIfAbsent(recency) { IntArrayList() }.add(idx)
    }

    return res.mapValuesTo(sortedMapOf()) { it.value.toArray() }
}

internal fun IntArray.recencyPartitions(
    recencies: IVectorReader,
    pageLimit: Int,
): SortedMap<InstantMicros, Selection> {
    if (isEmpty()) return sortedMapOf()

    val partCount = ceil(size.toDouble() / pageLimit).toInt()
    val partSize = ceil(size.toDouble() / partCount).toInt()

    val sortedByRecency =
        IntStream.range(0, size).boxed()
            .sorted { l, r -> recencies.getLong(r).compareTo(recencies.getLong(l)) }
            .mapToInt { it }
            .toArray()

    // split sortedByRecency into sub-arrays of size partSize
    val res = sortedMapOf<InstantMicros, Selection>()

    for (sortedIdx in sortedByRecency.indices step partSize) {
        val part = sortedByRecency.sliceArray(sortedIdx until (sortedIdx + partSize).coerceAtMost(size))
        val recency = recencies.getLong(part.first())
        res[recency] = part.sortedArray() // preserve the initial sort order
    }

    return res
}

@Suppress("unused")
@JvmOverloads
fun writeRelation(
    trieWriter: ITrieWriter,
    relation: RelationReader,
    recencies: IVectorReader,
    pageLimit: Int = 256,
) {
    val trieDataWriter = trieWriter.dataWriter
    val rowCopier = trieDataWriter.rowCopier(relation)
    val iidReader = relation.readerForName("xt\$iid")

    fun writeSubtree(depth: Int, sel: Selection): Int =
        if (Thread.interrupted()) throw InterruptedException()
        else if (sel.size <= pageLimit) {
            for (idx in sel) rowCopier.copyRow(idx)

            val pos = trieWriter.writeLeaf()
            trieDataWriter.clear()
            pos
        } else {
            fun writeRecencyBranch(parts: SortedMap<InstantMicros, Selection>): Int =
                trieWriter.writeRecencyBranch(
                    parts.mapValuesTo(sortedMapOf()) { innerSel ->
                        writeSubtree(depth + 1, innerSel.value)
                    }
                )

            // Year, IID[0], Quarter, IID[1], Month, IID[2..]
            // then, when we've run out of IID, we know that it's all versions of the same IID, so we page by recency
            when (depth) {
                0 -> writeRecencyBranch(sel.recencyPartitions(recencies, YEAR))
                2 -> writeRecencyBranch(sel.recencyPartitions(recencies, QUARTER))
                4 -> writeRecencyBranch(sel.recencyPartitions(recencies, MONTH))

                else -> {
                    val iidDepth = when (depth) {
                        1 -> 0; 3 -> 1
                        else -> depth - 3
                    }

                    if (iidDepth < 64) {
                        trieWriter.writeIidBranch(
                            sel.iidPartitions(iidReader, iidDepth)
                                .map { innerSel ->
                                    if (innerSel == null) -1 else writeSubtree(depth + 1, innerSel)
                                }
                                .toIntArray())
                    } else {
                        writeRecencyBranch(sel.recencyPartitions(recencies, pageLimit))
                    }
                }
            }
        }

    writeSubtree(0, IntArray(relation.rowCount()) { idx -> idx })

    trieWriter.end()
}

