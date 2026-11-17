package xtdb.operator.distinct

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.IntVector
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorTypes
import xtdb.expression.map.IndexHasher.Companion.hasher
import xtdb.expression.map.RelationMapBuilder
import xtdb.trie.MutableMemoryHashTrie
import java.util.function.IntBinaryOperator

class DistinctRelationMap(
    val allocator: BufferAllocator,
    val vecTypes: VectorTypes,
    val keyColumnNames: List<String>,
    private val storeFullBuildRel: Boolean,
    private val comparatorFactory: ComparatorFactory,
    pageLimit: Int = 16, levelBits: Int = 2
) : AutoCloseable {

    interface ComparatorFactory {
        fun buildEqui(l: VectorReader, r: VectorReader): IntBinaryOperator
    }

    private val accRel = Relation(allocator, vecTypes)
    private val keyCols = keyColumnNames.map { accRel[it] }

    private val hashColumn = IntVector.open(allocator, "xt/join-hash", false)
    private val batchHashes = IntVector.open(allocator, "xt/batch-hashes", false)
    private val batchHashScratch = IntVector.open(allocator, "xt/batch-hash-scratch", false)

    private var buildHashTrie =
        MutableMemoryHashTrie.builder(hashColumn)
            .setPageLimit(pageLimit).setLevelBits(levelBits)
            .build()

    fun andIBO(p1: IntBinaryOperator, p2: IntBinaryOperator): IntBinaryOperator =
        IntBinaryOperator { l, r ->
            val lRes = p1.applyAsInt(l, r)
            if (lRes == -1) -1 else minOf(lRes, p2.applyAsInt(l, r))
        }

    companion object {
        @JvmStatic
        fun returnedIdx(insertedIdx: Int): Int = -insertedIdx - 1

        @JvmStatic
        fun insertedIdx(returnedIdx: Int): Int = if (returnedIdx < 0) -returnedIdx - 1 else returnedIdx
    }

    /**
     * The hashes of [inRel]'s key columns, one per row, for [buildFromRelation].
     * They're held in a vector this map owns, valid until the next call.
     */
    fun hashesFor(inRel: RelationReader): IntVector =
        batchHashes.also { inRel.hasher(keyColumnNames).hashTo(it, batchHashScratch) }

    /** [hashes] holds [inRel]'s key-column hashes, row for row - from [hashesFor], or `IndexHasher.hashTo` over the same key columns. */
    @Suppress("NAME_SHADOWING")
    fun buildFromRelation(inRel: RelationReader, hashes: IntVector): RelationMapBuilder {
        val inKeyCols = keyColumnNames.map { inRel[it] }
        val inRel = if (storeFullBuildRel) inRel else RelationReader.from(inKeyCols)

        val comparator = keyCols.zip(inKeyCols)
            .map { (buildCol, inCol) -> comparatorFactory.buildEqui(buildCol, inCol) }
            .reduceOrNull(::andIBO)
            ?: IntBinaryOperator { _, _ -> 1 }

        val rowCopier = inRel.rowCopier(accRel)

        return object : RelationMapBuilder {
            override fun addIfNotPresent(inIdx: Int): Int {
                val hashCode = hashes.getInt(inIdx)

                val outIdxHashColumn = hashColumn.valueCount

                val (insertedIdx, newTrie) = buildHashTrie.addIfNotPresent(
                    hashCode,
                    outIdxHashColumn,
                    { testIdx -> comparator.applyAsInt(testIdx, inIdx) },
                    {
                        hashColumn.writeInt(hashCode)
                        rowCopier.copyRow(inIdx)
                    }
                )

                return if (insertedIdx == outIdxHashColumn) {
                    buildHashTrie = newTrie
                    returnedIdx(outIdxHashColumn)
                } else {
                    insertedIdx
                }
            }
        }
    }

    fun getBuiltRelation(): RelationReader = accRel

    override fun close() {
        accRel.close()
        hashColumn.close()
        batchHashes.close()
        batchHashScratch.close()
    }
}
