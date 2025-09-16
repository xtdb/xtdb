package xtdb.operator.join

import xtdb.arrow.RelationReader
import xtdb.expression.map.IndexHasher
import xtdb.expression.map.IndexHasher.Companion.hasher
import java.util.function.BiConsumer
import java.util.function.IntBinaryOperator
import java.util.function.IntConsumer

class ProbeSide(
    private val buildSide: BuildSide, val probeRel: RelationReader,
    val keyColNames: List<String>, private val comparator: IntBinaryOperator,
) {

    internal val buildRel = buildSide.builtRel
    val nullRowIdx get() = buildSide.nullRowIdx
    val rowCount = probeRel.rowCount

    private val hasher = probeRel.hasher(keyColNames)

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
