package xtdb.operator.scan

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.*
import xtdb.arrow.Vector.Companion.openVector
import xtdb.time.TEMPORAL_COL_NAMES
import xtdb.trie.ColumnName
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.safeMap
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

class BitemporalConsumer private constructor(
    private val rels: List<RelBuilder>, private val colNames: List<ColumnName>
) : AutoCloseable {

    private class TemporalVecs private constructor(
        private val validFromVec: VectorWriter?, private val validToVec: VectorWriter?,
        private val systemFromVec: VectorWriter?, private val systemToVec: VectorWriter?
    ) : AutoCloseable {
        val vecs = listOfNotNull(validFromVec, validToVec, systemFromVec, systemToVec)

        fun write(validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long) {
            validFromVec?.writeLong(validFrom)
            validToVec?.let { if (validTo == MAX_LONG) it.writeNull() else it.writeLong(validTo) }
            systemFromVec?.writeLong(systemFrom)
            systemToVec?.let { if (systemTo == MAX_LONG) it.writeNull() else it.writeLong(systemTo) }
        }

        fun padTo(rowCount: Int) {
            for (vec in vecs) repeat(rowCount - vec.valueCount) { vec.writeUndefined() }
        }

        override fun close() = vecs.closeAll()

        companion object {
            private inline fun <R> unlessNot(condition: Boolean, block: () -> R): R? =
                if (condition) block() else null

            fun open(
                al: BufferAllocator,
                hasValidFrom: Boolean, hasValidTo: Boolean,
                hasSystemFrom: Boolean, hasSystemTo: Boolean,
            ): TemporalVecs =
                mutableListOf<VectorWriter>().closeAllOnCatch { temporalVecs ->
                    fun openTemporalVec(name: String, nullable: Boolean): VectorWriter {
                        val vec = al.openVector(name, maybe(VectorType.INSTANT, nullable))
                        temporalVecs.add(vec)
                        return vec
                    }

                    TemporalVecs(
                        validFromVec = unlessNot(hasValidFrom) { openTemporalVec("_valid_from", false) },
                        validToVec = unlessNot(hasValidTo) { openTemporalVec("_valid_to", true) },
                        systemFromVec = unlessNot(hasSystemFrom) { openTemporalVec("_system_from", false) },
                        systemToVec = unlessNot(hasSystemTo) { openTemporalVec("_system_to", true) }
                    )
                }
        }
    }

    private sealed class RelBuilder(protected val temporalVecs: TemporalVecs) : AutoCloseable {
        abstract fun accept(idx: Int, validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long)
        abstract fun build(): RelationReader

        override fun close() = temporalVecs.close()
    }

    private class SelectingRelBuilder(
        private val rel: RelationReader, private val contentColNames: List<ColumnName>, temporalVecs: TemporalVecs
    ) : RelBuilder(temporalVecs) {
        private val contentSel = IntArrayList()

        override fun accept(idx: Int, validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long) {
            contentSel.add(idx)
            temporalVecs.write(validFrom, validTo, systemFrom, systemTo)
        }

        override fun build(): RelationReader {
            val selArray = contentSel.toArray()

            val putReader =
                // an erase-only leaf writes `put` as an empty struct (see #4577) — treat that as
                // "no puts here" rather than merging its absent columns in. Checked via the struct's
                // children rather than `_id`-presence, matching SegmentMerge's emptiness test: not
                // every table mandates an `_id`, and the two predicates agree for every one that does.
                rel["op"].vectorForOrNull("put")
                    ?.takeIf { putVec -> !putVec.keyNames.isNullOrEmpty() }
                    ?.select(selArray)

            return RelationReader.from(
                contentColNames
                    .map {
                        if (it == "_iid") rel["_iid"].select(selArray)
                        else putReader?.vectorForOrNull(it) ?: NullVector(it, true, selArray.size)
                    }
                    .plus(temporalVecs.vecs),
                selArray.size)
        }
    }

    /**
     * Every column, temporal ones included, is at the `put` struct's row coordinates, and the mask sets the accepted rows.
     * Temporal rows the mask leaves out are undefined.
     */
    private class MaskingRelBuilder(
        private val putLeg: DenseUnionVector.LegVector, private val contentColNames: List<ColumnName>,
        temporalVecs: TemporalVecs
    ) : RelBuilder(temporalVecs) {
        private val putRows = putLeg.inner
        private val mask = RowMask.Builder(putRows.valueCount)

        override fun accept(idx: Int, validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long) {
            val putRow = putLeg.getOffset(idx)
            mask.add(putRow)
            temporalVecs.padTo(putRow)
            temporalVecs.write(validFrom, validTo, systemFrom, systemTo)
        }

        override fun build(): RelationReader {
            val rowCount = putRows.valueCount
            temporalVecs.padTo(rowCount)

            val cols = contentColNames.map { putRows.vectorForOrNull(it) ?: NullVector(it, true, rowCount) }

            return MaskedRelation.of(RelationReader.from(cols + temporalVecs.vecs, rowCount), mask.build())
        }
    }

    companion object {
        /**
         * @param eachRowOnceAscending each page's rows are accepted at most once, in ascending order.
         */
        fun open(
            al: BufferAllocator, rels: List<RelationReader>, colNames: List<ColumnName>,
            eachRowOnceAscending: Boolean
        ): BitemporalConsumer {
            val hasValidFrom = "_valid_from" in colNames
            val hasValidTo = "_valid_to" in colNames
            val hasSystemFrom = "_system_from" in colNames
            val hasSystemTo = "_system_to" in colNames

            val contentColNames = colNames.filterNot { it in TEMPORAL_COL_NAMES }

            return rels.safeMap { rel ->
                TemporalVecs.open(al, hasValidFrom, hasValidTo, hasSystemFrom, hasSystemTo).closeOnCatch { temporalVecs ->
                    val putLeg = (rel["op"].vectorForOrNull("put") as? DenseUnionVector.LegVector)
                        ?.takeIf { eachRowOnceAscending && "_iid" !in contentColNames && !it.keyNames.isNullOrEmpty() }

                    if (putLeg != null) MaskingRelBuilder(putLeg, contentColNames, temporalVecs)
                    else SelectingRelBuilder(rel, contentColNames, temporalVecs)
                }
            }.closeAllOnCatch {
                BitemporalConsumer(it, colNames)
            }
        }
    }

    private var rowCount = 0

    fun accept(
        relIdx: Int, rowIdx: Int,
        validFrom: Long, validTo: Long,
        systemFrom: Long, systemTo: Long
    ) {
        rels[relIdx].accept(rowIdx, validFrom, validTo, systemFrom, systemTo)
        rowCount++
    }

    fun build() = rels.map { it.build() }

    override fun close() = rels.closeAll()

}
