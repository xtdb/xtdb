package xtdb.operator.join

import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.expression.map.IndexHasher
import java.util.function.BiConsumer
import java.util.function.IntBinaryOperator
import java.util.function.IntConsumer

class ProbeSide(
    private val buildSide: BuildSide, val probeRel: RelationReader,
    val keyColNames: List<String>, private val comparatorFactory: ComparatorFactory,
) {

    interface ComparatorFactory {
        fun buildEqui(buildCol: VectorReader, probeCol: VectorReader): IntBinaryOperator
        fun buildTheta(buildRel: RelationReader, probeRel: RelationReader): IntBinaryOperator?
    }

    internal val buildRel = buildSide.builtRel
    val rowCount = probeRel.rowCount

    private val probeKeyCols = keyColNames.map { probeRel[it] }

    private fun andIBO(p1: IntBinaryOperator, p2: IntBinaryOperator) = IntBinaryOperator { l, r ->
        val lRes = p1.applyAsInt(l, r)
        if (lRes == -1) -1 else minOf(lRes, p2.applyAsInt(l, r))
    }

    private val comparator =
        buildSide.keyColNames
            .map { buildRel[it] }.zip(probeKeyCols)
            .map { (buildCol, probeCol) -> comparatorFactory.buildEqui(buildCol, probeCol) }
            .plus(listOfNotNull(comparatorFactory.buildTheta(buildRel, probeRel)))
            .reduceOrNull(::andIBO)
            ?: IntBinaryOperator { _, _ -> 1 }

    private val hasher = IndexHasher.fromCols(probeKeyCols)

    fun forEachIndexOf(c: BiConsumer<Int, Int>, removeOnMatch: Boolean) =
        repeat(rowCount) { probeIdx ->
            val buildIdx = buildSide.indexOf(
                hasher.hashCode(probeIdx),
                { buildIdx -> comparator.applyAsInt(buildIdx, probeIdx) },
                removeOnMatch
            )
            c.accept(probeIdx, buildIdx)
            if (buildIdx >= 0) buildSide.addMatch(buildIdx)
        }

    fun forEachMatch(probeIdx: Int, c: IntConsumer) {
        val hashCode = hasher.hashCode(probeIdx)

        buildSide.forEachMatch(hashCode) { idx ->
            if (comparator.applyAsInt(idx, probeIdx) == 1) {
                c.accept(idx)
                buildSide.addMatch(idx)
            }
        }
    }

    fun matches(probeIdx: Int): Int {
        // TODO: This doesn't use the hashTries, still a nested loop join
        var acc = -1
        val buildRowCount = buildRel.rowCount
        for (buildIdx in 0 until buildRowCount) {
            val res = comparator.applyAsInt(buildIdx, probeIdx)
            if (res == 1) return 1
            acc = maxOf(acc, res)
        }
        return acc
    }

}
