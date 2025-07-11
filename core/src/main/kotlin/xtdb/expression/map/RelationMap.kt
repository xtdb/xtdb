package xtdb.expression.map

import clojure.lang.IFn
import com.carrotsearch.hppc.IntObjectHashMap
import java.util.function.IntBinaryOperator
import java.util.function.IntConsumer
import org.apache.arrow.memory.util.hash.MurmurHasher
import org.apache.arrow.vector.types.pojo.Field
import org.roaringbitmap.RoaringBitmap
import xtdb.*

import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.VectorReader
import xtdb.util.Hasher
import xtdb.util.requiringResolve

class RelationMap(
    val buildFields : Map<String, Field>,
    val buildKeyColumnNames: List<String>,
    val probeFields : Map<String, Field>,
    val probeKeyColumnNames:  List<String>,
    private val storeFullBuildRel : Boolean,
    private val relWriter: RelationWriter,
    private val buildKeyCols: List<VectorReader>,
    private val hashToBitmap: IntObjectHashMap<RoaringBitmap>,
    private val nilKeysEqual: Boolean = false,
    private val thetaExpr: Any? = null,
    private val paramTypes: Map<String, Any> = emptyMap(),
    private val args: RelationReader? = null
) : AutoCloseable {

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

    fun RoaringBitmap?.findInHashBitmap(comparator: IntBinaryOperator, idx: Int, removeOnMatch: Boolean): Int {
        if (this == null) return -1

        val iterator = this.intIterator
        while (iterator.hasNext()) {
            val testIdx = iterator.next()
            if (comparator.applyAsInt(idx, testIdx) == 1) {
                if (removeOnMatch) {
                    this.remove(testIdx)
                }
                return testIdx
            }
        }
        return -1
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

    fun computeHashBitmap(rowHash: Int) =
        hashToBitmap.get(rowHash) ?: run {
            val bitmap = RoaringBitmap()
            hashToBitmap.put(rowHash, bitmap)
            bitmap
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

        fun add(hashBitmap: RoaringBitmap, idx: Int): Int {
            val outIdx = rowCopier.copyRow(idx)
            hashBitmap.add(outIdx)
            return returnedIdx(outIdx)
        }


        return object : RelationMapBuilder {
            override fun add(inIdx: Int) {
                add(computeHashBitmap(hasher.hashCode(inIdx)), inIdx);
            }

            override fun addIfNotPresent(inIdx: Int): Int {
                val hashBitmap = computeHashBitmap(hasher.hashCode(inIdx))
                val outIdx = hashBitmap.findInHashBitmap(comparatorLazy, inIdx, false)
                return if (outIdx >= 0) { outIdx } else { add(hashBitmap, inIdx) }
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
            override fun indexOf(inIdx: Int, removeOnMatch: Boolean): Int{
                val hashCode = hasher.hashCode(inIdx)
                val hashBitmap = hashToBitmap.get(hashCode)

                return hashBitmap.findInHashBitmap(comparator, inIdx, removeOnMatch)
            }

            override fun forEachMatch(inIdx: Int, c: IntConsumer) {
                val hashCode = hasher.hashCode(inIdx)
                val hashBitmap = hashToBitmap.get(hashCode)

                hashBitmap?.let { bitmap ->
                    val iterator = bitmap.intIterator
                    while (iterator.hasNext()) {
                        val outIdx = iterator.next()
                        if (comparator.applyAsInt(inIdx.toInt(), outIdx) == 1) {
                            c.accept(outIdx)
                        }
                    }
                }
            }

            override fun matches(inIdx: Int): Int {
                // TODO: This doesn't use hashmaps, still a nested loop join
                var acc = -1
                val buildRowCount = buildRel.rowCount
                for (buildIdx in 0 until buildRowCount.toInt()) {
                    val res = comparator.applyAsInt(inIdx.toInt(), buildIdx)
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
    }
}
