package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.IntVector
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.expression.map.IndexHasher
import xtdb.trie.MutableMemoryHashTrie
import xtdb.vector.OldRelationWriter
import java.util.function.IntConsumer
import java.util.function.IntUnaryOperator

internal const val NULL_ROW_IDX = 0

class BuildSide @JvmOverloads constructor(
    val al: BufferAllocator, val schema: Schema, val keyColNames: List<String>, val matchedBuildIdxs: RoaringBitmap?,
    withNilRow: Boolean, pageLimit: Int = 64, levelBits: Int = 4
) : AutoCloseable {
    private val relWriter = OldRelationWriter(al, schema)

    private val hashColumn: IntVector = IntVector(al, "xt/join-hash", false)

    var buildHashTrie: MutableMemoryHashTrie
        private set

    init {
        if (withNilRow) {
            relWriter.endRow()
            hashColumn.writeInt(0)
        }

        buildHashTrie =
            MutableMemoryHashTrie.builder(hashColumn.asReader)
                .setPageLimit(pageLimit).setLevelBits(levelBits)
                .build()
    }

    @Suppress("NAME_SHADOWING")
    fun append(inRel: RelationReader) {
        val inKeyCols = keyColNames.map { inRel.vectorForOrNull(it) as VectorReader }

        val hasher = IndexHasher.fromCols(inKeyCols)
        val rowCopier = relWriter.rowCopier(inRel)

        repeat(inRel.rowCount) { inIdx ->
            // outIndex and used index for hashColumn should be identical
            val outIdxHashColumn = hashColumn.valueCount
            hashColumn.writeInt(hasher.hashCode(inIdx))
            buildHashTrie += outIdxHashColumn

            val outIdx = rowCopier.copyRow(inIdx)
            assert(outIdx == outIdxHashColumn) {
                "Expected outIdx $outIdx to match hashColumn valueCount $outIdxHashColumn"
            }
        }
    }

    val builtRel get() = relWriter.asReader

    fun addMatch(idx: Int) = matchedBuildIdxs?.add(idx)

    fun indexOf(hashCode: Int, cmp: IntUnaryOperator, removeOnMatch: Boolean): Int =
        buildHashTrie.findValue(hashCode, cmp, removeOnMatch)

    fun forEachMatch(hashCode: Int, c: IntConsumer) = buildHashTrie.forEachMatch(hashCode, c)

    override fun close() {
        relWriter.close()
        hashColumn.close()
    }
}