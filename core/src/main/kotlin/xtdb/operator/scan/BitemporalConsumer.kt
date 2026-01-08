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
import xtdb.util.safeMap
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

class BitemporalConsumer private constructor(
    private val rels: List<RelBuilder>, private val colNames: List<ColumnName>
) : AutoCloseable {

    class RelBuilder private constructor(
        private val rel: RelationReader, private val contentColNames: List<ColumnName>,
        private val validFromVec: VectorWriter?, private val validToVec: VectorWriter?,
        private val systemFromVec: VectorWriter?, private val systemToVec: VectorWriter?
    ) : AutoCloseable {
        private val contentSel = IntArrayList()

        companion object {
            private inline fun <R> unlessNot(condition: Boolean, block: () -> R): R? =
                if (condition) block() else null

            fun open(
                al: BufferAllocator, rel: RelationReader, contentColNames: List<ColumnName>,
                hasValidFrom: Boolean, hasValidTo: Boolean,
                hasSystemFrom: Boolean, hasSystemTo: Boolean,
            ): RelBuilder =
                mutableListOf<VectorWriter>().closeAllOnCatch { temporalVecs ->
                    fun openTemporalVec(name: String, nullable: Boolean): VectorWriter {
                        val vec = al.openVector(name, maybe(VectorType.INSTANT, nullable))
                        temporalVecs.add(vec)
                        return vec
                    }

                    RelBuilder(
                        rel, contentColNames,
                        validFromVec = unlessNot(hasValidFrom) { openTemporalVec("_valid_from", false) },
                        validToVec = unlessNot(hasValidTo) { openTemporalVec("_valid_to", true) },
                        systemFromVec = unlessNot(hasSystemFrom) { openTemporalVec("_system_from", false) },
                        systemToVec = unlessNot(hasSystemTo) { openTemporalVec("_system_to", true) }
                    )
                }
        }

        fun accept(
            idx: Int,
            validFrom: Long, validTo: Long,
            systemFrom: Long, systemTo: Long
        ) {
            contentSel.add(idx)

            validFromVec?.writeLong(validFrom)
            validToVec?.let { if (validTo == MAX_LONG) it.writeNull() else it.writeLong(validTo) }
            systemFromVec?.writeLong(systemFrom)
            systemToVec?.let { if (systemTo == MAX_LONG) it.writeNull() else it.writeLong(systemTo) }
        }

        fun build(): RelationReader {
            val selArray = contentSel.toArray()

            val putReader =
                rel["op"].vectorForOrNull("put")
                    ?.takeIf { putVec -> putVec.vectorForOrNull("_id") != null }
                    ?.select(selArray)

            return RelationReader.from(
                contentColNames
                    .map {
                        if (it == "_iid") rel["_iid"].select(selArray)
                        else putReader?.vectorForOrNull(it) ?: NullVector(it, true, selArray.size)
                    }
                    .plus(listOfNotNull(validFromVec, validToVec, systemFromVec, systemToVec)),
                selArray.size)
        }

        override fun close() {
            validFromVec?.close()
            validToVec?.close()
            systemFromVec?.close()
            systemToVec?.close()
        }
    }

    companion object {
        fun open(al: BufferAllocator, rels: List<RelationReader>, colNames: List<ColumnName>): BitemporalConsumer {
            val hasValidFrom = "_valid_from" in colNames
            val hasValidTo = "_valid_to" in colNames
            val hasSystemFrom = "_system_from" in colNames
            val hasSystemTo = "_system_to" in colNames

            val contentColNames = colNames.filterNot { it in TEMPORAL_COL_NAMES }

            return rels.safeMap { rel ->
                RelBuilder.open(
                    al, rel, contentColNames,
                    hasValidFrom, hasValidTo,
                    hasSystemFrom, hasSystemTo
                )
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