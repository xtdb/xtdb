package xtdb.operator.join

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.vector.BitVector
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.error.Incorrect
import xtdb.expression.map.IndexHasher
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

    private val buildRel = buildSide.builtRel
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

    // TODO one could very likely do a similar thing to a merge sort phase with the buildHashTrie and a probeHashTrie
    fun indexOf(probeIdx: Int, removeOnMatch: Boolean): Int =
        buildSide.indexOf(
            hasher.hashCode(probeIdx),
            { buildIdx -> comparator.applyAsInt(buildIdx, probeIdx) },
            removeOnMatch
        )

    fun forEachMatch(probeIdx: Int, c: IntConsumer) {
        val hashCode = hasher.hashCode(probeIdx)

        buildSide.forEachMatch(hashCode) { idx ->
            if (comparator.applyAsInt(idx, probeIdx) == 1) {
                c.accept(idx)
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

    fun innerJoin(): RelationReader {
        val matchingBuildIdxs = IntArrayList()
        val matchingProbeIdxs = IntArrayList()

        repeat(rowCount) { probeIdx ->
            forEachMatch(probeIdx) { buildIdx ->
                matchingBuildIdxs.add(buildIdx)
                matchingProbeIdxs.add(probeIdx)
            }
        }

        return RelationReader.concatCols(
            buildRel.select(matchingBuildIdxs.toArray()),
            probeRel.select(matchingProbeIdxs.toArray())
        )
    }

    fun outerJoin(matchedBuildIdxs: RoaringBitmap?, isFlippedLeftOuterJoin: Boolean): RelationReader {
        val matchingBuildIdxs = IntArrayList()
        val matchingProbeIdxs = IntArrayList()

        repeat(rowCount) { probeIdx ->
            var matched = false

            forEachMatch(probeIdx) { buildIdx ->
                matched = true
                matchingBuildIdxs.add(buildIdx)
                matchingProbeIdxs.add(probeIdx)
            }

            if (!isFlippedLeftOuterJoin && !matched) {
                matchingBuildIdxs.add(NULL_ROW_IDX)
                matchingProbeIdxs.add(probeIdx)
            }
        }

        matchedBuildIdxs?.add(*matchingBuildIdxs.toArray())

        val outBuild = buildRel.select(matchingBuildIdxs.toArray())
        val outProbe = probeRel.select(matchingProbeIdxs.toArray())

        return if (isFlippedLeftOuterJoin)
            RelationReader.concatCols(outProbe, outBuild)
        else
            RelationReader.concatCols(outBuild, outProbe)
    }

    internal val semiJoinSelection
        get() =
            IntArrayList().let { sel ->
                repeat(rowCount) { if (indexOf(it, false) >= 0) sel.add(it) }

                sel.toArray()
            }

    fun semiJoin() = probeRel.select(semiJoinSelection)

    internal val antiJoinSelection
        get() = IntArrayList().let { sel ->
            repeat(rowCount) { if (matches(it) < 0) sel.add(it) }

            sel.toArray()
        }

    fun antiJoin() = probeRel.select(antiJoinSelection)

    fun mark(markCol: BitVector) {
        repeat(rowCount) { probeIdx ->
            when (matches(probeIdx)) {
                0 -> markCol.setNull(probeIdx)
                1 -> markCol.set(probeIdx, 1)
                -1 -> markCol.set(probeIdx, 0)
            }
        }
    }

    fun singleJoin(): RelationReader {
        val matchingBuildIdxs = IntArrayList()
        val matchingProbeIdxs = IntArrayList()

        repeat(rowCount) { probeIdx ->
            var matched = false
            forEachMatch(probeIdx) { buildIdx ->
                if (matched)
                    throw Incorrect("cardinality violation", "xtdb.single-join/cardinality-violation")
                matched = true

                matchingBuildIdxs.add(buildIdx)
                matchingProbeIdxs.add(probeIdx)
            }

            if (!matched) {
                matchingBuildIdxs.add(NULL_ROW_IDX)
                matchingProbeIdxs.add(probeIdx)
            }
        }

        return RelationReader.concatCols(
            buildRel.select(matchingBuildIdxs.toArray()),
            probeRel.select(matchingProbeIdxs.toArray())
        )
    }
}