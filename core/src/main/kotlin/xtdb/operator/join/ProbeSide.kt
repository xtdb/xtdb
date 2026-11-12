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

    fun indexOf(probeIdx: Int, removeOnMatch: Boolean): Int {
        val buildIdx = buildSide.indexOf(
            hasher.hashCode(probeIdx),
            { buildIdx -> comparator.applyAsInt(buildIdx, probeIdx) },
            removeOnMatch
        )
        if (buildIdx >= 0) buildSide.addMatch(buildIdx)
        return buildIdx
    }

    fun forEachIndexOf(c: BiConsumer<Int, Int>, removeOnMatch: Boolean) =
        repeat(rowCount) { probeIdx -> c.accept(probeIdx, indexOf(probeIdx, removeOnMatch)) }

    fun iterator(probeIdx: Int): IntIterator {
        val iter = candidates(probeIdx)

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

    /** The build rows sharing this probe row's hash, unfiltered: [iterator] keeps only the rows that [passes]. */
    fun candidates(probeIdx: Int): IntIterator = buildSide.iterator(hasher.hashCode(probeIdx))

    /** Whether the build and probe rows satisfy the join's conditions: an unknown result doesn't. */
    fun passes(buildIdx: Int, probeIdx: Int): Boolean = comparator.applyAsInt(buildIdx, probeIdx) == 1
}
