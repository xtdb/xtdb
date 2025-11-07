package xtdb.operator.join

import xtdb.arrow.RelationReader
import xtdb.expression.map.IndexHasher
import xtdb.expression.map.IndexHasher.Companion.hasher
import java.util.function.BiConsumer
import java.util.function.IntBinaryOperator

class ProbeSide(
    private val buildSide: BuildSide, val probeRel: RelationReader,
    val keyColNames: List<String>, private val comparator: IntBinaryOperator,
) {

    internal val buildRel = buildSide.dataRel
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

    fun iterator(probeIdx: Int): IntIterator {
        val hashCode = hasher.hashCode(probeIdx)
        val iter = buildSide.iterator(hashCode)

        return object : IntIterator() {
            private var nextValue = -1
            private var hasNextValue = false

            override fun hasNext(): Boolean {
                if (hasNextValue) return true

                while (iter.hasNext()) {
                    val idx = iter.nextInt()
                    if (comparator.applyAsInt(idx, probeIdx) == 1) {
                        nextValue = idx
                        hasNextValue = true
                        buildSide.addMatch(idx)
                        return true
                    }
                }
                return false
            }

            override fun nextInt(): Int {
                if (!hasNext()) throw NoSuchElementException()
                hasNextValue = false
                return nextValue
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
