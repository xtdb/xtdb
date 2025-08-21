package xtdb.operator.join

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.vector.BitVector
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.RelationReader
import xtdb.error.Incorrect
import xtdb.operator.join.JoinType.OuterJoinType.*

fun interface JoinType {
    fun ProbeSide.probe(): RelationReader

    val outerJoinType: OuterJoinType? get() = null

    enum class OuterJoinType {
        LEFT, LEFT_FLIPPED, FULL
    }

    class Outer(private val matchedBuildIdxs: RoaringBitmap?, override val outerJoinType: OuterJoinType) : JoinType {

        override fun ProbeSide.probe(): RelationReader {
            val matchingBuildIdxs = IntArrayList()
            val matchingProbeIdxs = IntArrayList()

            repeat(rowCount) { probeIdx ->
                var matched = false

                forEachMatch(probeIdx) { buildIdx ->
                    matched = true
                    matchingBuildIdxs.add(buildIdx)
                    matchingProbeIdxs.add(probeIdx)
                }

                if (outerJoinType != LEFT_FLIPPED && !matched) {
                    matchingBuildIdxs.add(NULL_ROW_IDX)
                    matchingProbeIdxs.add(probeIdx)
                }
            }

            matchedBuildIdxs?.add(*matchingBuildIdxs.toArray())

            val outBuild = buildRel.select(matchingBuildIdxs.toArray())
            val outProbe = probeRel.select(matchingProbeIdxs.toArray())

            return if (outerJoinType == LEFT_FLIPPED)
                RelationReader.concatCols(outProbe, outBuild)
            else
                RelationReader.concatCols(outBuild, outProbe)
        }

    }

    companion object {
        @JvmField
        val INNER = JoinType {
            val matchingBuildIdxs = IntArrayList()
            val matchingProbeIdxs = IntArrayList()

            repeat(rowCount) { probeIdx ->
                forEachMatch(probeIdx) { buildIdx ->
                    matchingBuildIdxs.add(buildIdx)
                    matchingProbeIdxs.add(probeIdx)
                }
            }

            RelationReader.concatCols(
                buildRel.select(matchingBuildIdxs.toArray()),
                probeRel.select(matchingProbeIdxs.toArray())
            )
        }

        @JvmStatic
        fun leftOuter(matchedBuildIdxs: RoaringBitmap?) = Outer(matchedBuildIdxs, LEFT)

        @JvmStatic
        fun leftOuterFlipped(matchedBuildIdxs: RoaringBitmap?) = Outer(matchedBuildIdxs, LEFT_FLIPPED)

        @JvmStatic
        fun fullOuter(matchedBuildIdxs: RoaringBitmap?) = Outer(matchedBuildIdxs, FULL)

        @JvmStatic
        fun ProbeSide.mark(markCol: BitVector) {
            repeat(rowCount) { probeIdx ->
                when (matches(probeIdx)) {
                    0 -> markCol.setNull(probeIdx)
                    1 -> markCol.set(probeIdx, 1)
                    -1 -> markCol.set(probeIdx, 0)
                }
            }
        }

        internal fun ProbeSide.semiJoinSelection() =
            IntArrayList().let { sel ->
                repeat(rowCount) {
                    if (indexOf(it, false) >= 0) sel.add(it)
                }

                sel.toArray()
            }

        @JvmField
        val SEMI = JoinType { probeRel.select(semiJoinSelection()) }

        internal fun ProbeSide.antiJoinSelection() =
            IntArrayList().let { sel ->
                repeat(rowCount) { if (matches(it) < 0) sel.add(it) }

                sel.toArray()
            }

        @JvmField
        val ANTI = JoinType { probeRel.select(antiJoinSelection()) }

        @JvmField
        val SINGLE = JoinType {
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

            RelationReader.concatCols(
                buildRel.select(matchingBuildIdxs.toArray()),
                probeRel.select(matchingProbeIdxs.toArray())
            )
        }
    }
}