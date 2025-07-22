package xtdb.expression.map

import clojure.lang.IFn
import org.apache.arrow.memory.BufferAllocator
import java.util.function.IntBinaryOperator
import java.util.function.IntConsumer
import org.apache.arrow.memory.util.hash.MurmurHasher
import org.apache.arrow.vector.types.pojo.Field
import xtdb.*
import xtdb.arrow.IntVector

import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.VectorReader
import xtdb.trie.MutableMemoryHashTrie
import xtdb.util.Hasher
import xtdb.util.requiringResolve

class RelationMap(
    val allocator: BufferAllocator,
    val buildFields : Map<String, Field>,
    val buildKeyColumnNames: List<String>,
    val probeFields : Map<String, Field>,
    val probeKeyColumnNames:  List<String>,
    private val storeFullBuildRel : Boolean,
    private val relWriter: RelationWriter,
    private val buildKeyCols: List<VectorReader>,
    private val nilKeysEqual: Boolean = false,
    private val thetaExpr: Any? = null,
    private val paramTypes: Map<String, Any> = emptyMap(),
    private val args: RelationReader? = null,
    withNilRow: Boolean = false,
    pageLimit: Int = 16,
    levelBits: Int = 2
) : AutoCloseable {

    private val hashColumn: IntVector = IntVector(allocator, "xt/join-hash", false)
    private var buildHashTrie : MutableMemoryHashTrie

    init {
        if (withNilRow) hashColumn.writeInt(0)
        buildHashTrie = MutableMemoryHashTrie.builder(hashColumn.asReader).setPageLimit(pageLimit).setLevelBits(levelBits).build()
    }


    private val equiComparatorFn = requiringResolve("xtdb.expression.map/->equi-comparator") as IFn
    private val thetaComparatorFn = requiringResolve("xtdb.expression.map/->theta-comparator") as IFn

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
        return IndexHasher { index -> cols.foldRight(0) { col, acc -> MurmurHasher.combineHashCode(acc, col.hashCode(index, hasher)) } }
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

        // NOTE: we might not need to compute comparator if the caller never requires addIfNotPresent (e.g. joins)
        val comparatorLazy by lazy {
            val equiComparators = buildKeyCols.zip(inKeyCols).map { (buildCol, inCol) ->
                equiComparatorFn.invoke(
                    inCol, buildCol, args,
                    mapOf(
                        "nil-keys-equal?".kw to nilKeysEqual,
                        "param-types".kw to paramTypes
                    ).toClojureMap()
                ) as IntBinaryOperator
            }
            equiComparators.reduceOrNull({ acc, comp -> andIBO(acc, comp) }) ?: andIBO()
        }

        val hasher = createHasher(inKeyCols)
        val rowCopier = relWriter.rowCopier(inRel)

        return object : RelationMapBuilder {
            override fun add(inIdx: Int) {
                // outIndex and used index for hashColumn should be identical
                val outIdxHashColumn = hashColumn.valueCount
                hashColumn.writeInt(hasher.hashCode(inIdx))
                buildHashTrie += outIdxHashColumn

                val outIdx = rowCopier.copyRow(inIdx)
                assert(outIdx == outIdxHashColumn) {
                    "Expected outIdx $outIdx to match hashColumn valueCount $outIdxHashColumn"
                }
            }

            override fun addIfNotPresent(inIdx: Int): Int {
                val hashCode = hasher.hashCode(inIdx)
                val outIdxHashColumn = hashColumn.valueCount
                val (insertedIdx, newTrie) = buildHashTrie.addIfNotPresent(
                    hashCode,
                    outIdxHashColumn,
                    { testIdx -> comparatorLazy.applyAsInt(inIdx, testIdx) },
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

    fun probeFromRelation(probeRel: RelationReader): RelationMapProber {
        val buildRel = getBuiltRelation()
        val probeKeyCols = probeKeyColumnNames.map { probeRel.vectorForOrNull(it) as VectorReader }

        // Create equi-comparators for key columns
        val equiComparators = buildKeyCols.zip(probeKeyCols).map { (buildCol, probeCol) ->
            equiComparatorFn.invoke(
                probeCol, buildCol, args,
                mapOf(
                    "nil-keys-equal?".kw to nilKeysEqual,
                    "param-types".kw to paramTypes
                ).toClojureMap()
            ) as IntBinaryOperator
        }

        // Add theta comparator if needed
        val comparator = if (thetaExpr != null) {
            val thetaComparator = thetaComparatorFn.invoke(
                probeRel, buildRel, thetaExpr, args,
                mapOf(
                    "build-fields".kw to buildFields.mapKeys { it.key.symbol } .toClojureMap() ,
                    "probe-fields".kw to probeFields.mapKeys { it.key.symbol } .toClojureMap(),
                    "param-types".kw to paramTypes.mapKeys { it.key.symbol } .toClojureMap()
                ).toClojureMap()
            ) as IntBinaryOperator

            (equiComparators + thetaComparator).reduceOrNull({ acc, comp ->
                andIBO(acc, comp)
            }) ?: andIBO()
        } else {
            equiComparators.reduceOrNull({ acc, comp ->
                andIBO(acc, comp)
            }) ?: andIBO()
        }

        val hasher = createHasher(probeKeyCols)

        return object : RelationMapProber {
            // TODO one could very likely do a similar thing to a merge sort phase with the buildHashTrie and a probeHashTrie
            override fun indexOf(inIdx: Int, removeOnMatch: Boolean): Int{
                val hashCode = hasher.hashCode(inIdx)
                return buildHashTrie.findValue(hashCode, { testIdx -> comparator.applyAsInt(inIdx, testIdx) }, removeOnMatch)
            }

            override fun forEachMatch(inIdx: Int, c: IntConsumer) {
                val hashCode = hasher.hashCode(inIdx)
                val candidates = buildHashTrie.findCandidates(hashCode)
                for (i in 0 until candidates.size()) {
                    if (comparator.applyAsInt(inIdx, candidates[i]) == 1) {
                        c.accept(candidates[i])
                    }
                }
            }

            override fun matches(inIdx: Int): Int {
                // TODO: This doesn't use the hashTries, still a nested loop join
                var acc = -1
                val buildRowCount = buildRel.rowCount
                for (buildIdx in 0 until buildRowCount) {
                    val res = comparator.applyAsInt(inIdx, buildIdx)
                    if (res == 1) {
                        return 1
                    }
                    acc = maxOf(acc, res)
                }
                return acc
            }
        }
    }

    fun getBuiltRelation(): RelationReader = relWriter.asReader

    override fun close() {
        relWriter.close()
        hashColumn.close()
    }
}
