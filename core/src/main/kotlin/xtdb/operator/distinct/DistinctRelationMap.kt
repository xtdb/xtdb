package xtdb.operator.distinct

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.IntVector
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.expression.map.IndexHasher
import xtdb.expression.map.IndexHasher.Companion.hasher
import xtdb.expression.map.RelationMapBuilder
import xtdb.trie.MutableMemoryHashTrie
import java.util.function.IntBinaryOperator

class DistinctRelationMap(
    val allocator: BufferAllocator,
    val schema: Schema,
    val keyColumnNames: List<String>,
    private val storeFullBuildRel: Boolean,
    private val comparatorFactory: ComparatorFactory,
    pageLimit: Int = 16, levelBits: Int = 2
) : AutoCloseable {

    interface ComparatorFactory {
        fun buildEqui(l: VectorReader, r: VectorReader): IntBinaryOperator
    }

    private val accRel = Relation(allocator, schema)
    private val keyCols = keyColumnNames.map { accRel[it] }

    private val hashColumn = IntVector.open(allocator, "xt/join-hash", false)
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

    @Suppress("NAME_SHADOWING")
    fun buildFromRelation(inRel: RelationReader): RelationMapBuilder {
        val inKeyCols = keyColumnNames.map { inRel[it] }
        val inRel = if (storeFullBuildRel) inRel else RelationReader.from(inKeyCols)

        val comparator = keyCols.zip(inKeyCols)
            .map { (buildCol, inCol) -> comparatorFactory.buildEqui(buildCol, inCol) }
            .reduceOrNull(::andIBO)
            ?: IntBinaryOperator { _, _ -> 1 }

        val hasher = inRel.hasher(inKeyCols.map { it.name })
        val rowCopier = inRel.rowCopier(accRel)

        return object : RelationMapBuilder {
            override fun addIfNotPresent(inIdx: Int): Int {
                val hashCode = hasher.hashCode(inIdx)

                val outIdxHashColumn = hashColumn.valueCount

                val (insertedIdx, newTrie) = buildHashTrie.addIfNotPresent(
                    hashCode,
                    outIdxHashColumn,
                    { testIdx -> comparator.applyAsInt(testIdx, inIdx) },
                    {
                        hashColumn.writeInt(hashCode)
                        val outIdx = rowCopier.copyRow(inIdx)

                        assert(outIdx == outIdxHashColumn) {
                            "Expected outIdx $outIdx to match hashColumn valueCount $outIdxHashColumn"
                        }
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
    }
}
