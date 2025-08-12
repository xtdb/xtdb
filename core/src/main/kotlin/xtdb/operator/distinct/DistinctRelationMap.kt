package xtdb.operator.distinct

import clojure.lang.IFn
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.hash.MurmurHasher
import xtdb.arrow.IntVector
import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.VectorReader
import xtdb.expression.map.IndexHasher
import xtdb.expression.map.RelationMapBuilder
import xtdb.kw
import xtdb.toClojureMap
import xtdb.trie.MutableMemoryHashTrie
import xtdb.util.Hasher
import xtdb.util.requiringResolve
import java.util.function.IntBinaryOperator

class DistinctRelationMap(
    val allocator: BufferAllocator,
    val buildKeyColumnNames: List<String>,
    private val storeFullBuildRel: Boolean,
    private val relWriter: RelationWriter,
    private val buildKeyCols: List<VectorReader>,
    private val nilKeysEqual: Boolean = false,
    private val paramTypes: Map<String, Any> = emptyMap(),
    private val args: RelationReader? = null,
    pageLimit: Int = 16,
    levelBits: Int = 2
) : AutoCloseable {

    private val hashColumn: IntVector = IntVector(allocator, "xt/join-hash", false)
    private var buildHashTrie: MutableMemoryHashTrie

    init {
        buildHashTrie =
            MutableMemoryHashTrie.builder(hashColumn.asReader).setPageLimit(pageLimit).setLevelBits(levelBits).build()
    }

    private val equiComparatorFn = requiringResolve("xtdb.expression.map/->equi-comparator") as IFn

    @JvmOverloads
    fun andIBO(p1: IntBinaryOperator? = null, p2: IntBinaryOperator? = null): IntBinaryOperator {
        return if (p1 == null && p2 == null) {
            IntBinaryOperator { _, _ -> 1 }
        } else if (p1 != null && p2 != null) {
            IntBinaryOperator { l, r ->
                val lRes = p1.applyAsInt(l, r)
                if (lRes == -1) -1 else minOf(lRes, p2.applyAsInt(l, r))
            }
        } else {
            p1 ?: p2!!
        }
    }

    private fun createHasher(cols: List<VectorReader>): IndexHasher {
        val hasher = Hasher.Xx()
        return IndexHasher { index ->
            cols.foldRight(0) { col, acc ->
                MurmurHasher.combineHashCode(
                    acc,
                    col.hashCode(index, hasher)
                )
            }
        }
    }

    companion object {
        @JvmStatic
        fun returnedIdx(insertedIdx: Int): Int = -insertedIdx - 1

        @JvmStatic
        fun insertedIdx(returnedIdx: Int): Int = if (returnedIdx < 0) -returnedIdx - 1 else returnedIdx
    }

    @Suppress("NAME_SHADOWING")
    fun buildFromRelation(inRel: RelationReader): RelationMapBuilder {
        val inRel = if (storeFullBuildRel) {
            inRel
        } else {
            RelationReader.from(buildKeyColumnNames.map { inRel.vectorFor(it) })
        }

        val inKeyCols = buildKeyColumnNames.map { inRel.vectorForOrNull(it) as VectorReader }

        val equiComparators = buildKeyCols.zip(inKeyCols).map { (buildCol, inCol) ->
            equiComparatorFn.invoke(
                inCol, buildCol, args,
                mapOf(
                    "nil-keys-equal?".kw to nilKeysEqual,
                    "param-types".kw to paramTypes
                ).toClojureMap()
            ) as IntBinaryOperator
        }

        val comparator = equiComparators
            .reduceOrNull { acc, comp -> andIBO(acc, comp) }
            ?: andIBO()

        val hasher = createHasher(inKeyCols)
        val rowCopier = relWriter.rowCopier(inRel)

        return object : RelationMapBuilder {
            override fun addIfNotPresent(inIdx: Int): Int {
                val hashCode = hasher.hashCode(inIdx)
                val outIdxHashColumn = hashColumn.valueCount
                val (insertedIdx, newTrie) = buildHashTrie.addIfNotPresent(
                    hashCode,
                    outIdxHashColumn,
                    { testIdx -> comparator.applyAsInt(inIdx, testIdx) },
                    {
                        hashColumn.writeInt(hashCode)
                        val outIdx = rowCopier.copyRow(inIdx)

                        assert(outIdx == outIdxHashColumn) {
                            "Expected outIdx $outIdx to match hashColumn valueCount $outIdxHashColumn"
                        }
                    }
                )
                if (insertedIdx == outIdxHashColumn) {
                    buildHashTrie = newTrie
                    return returnedIdx(outIdxHashColumn)
                } else {
                    return insertedIdx
                }
            }
        }
    }

    fun getBuiltRelation(): RelationReader = relWriter.asReader

    override fun close() {
        relWriter.close()
        hashColumn.close()
    }
}