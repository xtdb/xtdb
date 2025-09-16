package xtdb.operator.join

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.vector.BitVector
import xtdb.arrow.RelationReader
import xtdb.error.Incorrect
import xtdb.operator.join.JoinType.OuterJoinType.*

interface JoinType {
    fun probe(probeSide: ProbeSide): RelationReader

    val outerJoinType: OuterJoinType? get() = null
    val joinTypeName: String

    enum class OuterJoinType {
        LEFT, LEFT_FLIPPED, FULL
    }

    class Outer(override val outerJoinType: OuterJoinType) : JoinType {
        
        override val joinTypeName: String get() = when (outerJoinType) {
            LEFT, LEFT_FLIPPED -> "left-join"
            FULL -> "full-join"
        }

        override fun probe(probeSide: ProbeSide): RelationReader {
            val matchingBuildIdxs = IntArrayList()
            val matchingProbeIdxs = IntArrayList()

            repeat(probeSide.rowCount) { probeIdx ->
                var matched = false

                probeSide.forEachMatch(probeIdx) { buildIdx ->
                    matched = true
                    matchingBuildIdxs.add(buildIdx)
                    matchingProbeIdxs.add(probeIdx)
                }

                if (outerJoinType != LEFT_FLIPPED && !matched) {
                    matchingBuildIdxs.add(probeSide.nullRowIdx)
                    matchingProbeIdxs.add(probeIdx)
                }
            }

            val outBuild = probeSide.buildRel.select(matchingBuildIdxs.toArray())
            val outProbe = probeSide.probeRel.select(matchingProbeIdxs.toArray())

            return if (outerJoinType == LEFT_FLIPPED)
                RelationReader.concatCols(outProbe, outBuild)
            else
                RelationReader.concatCols(outBuild, outProbe)
        }

    }

    companion object {
        @JvmField
        val INNER = object : JoinType {
            override val joinTypeName = "inner-join"
            
            override fun probe(probeSide: ProbeSide): RelationReader {
                val matchingBuildIdxs = IntArrayList()
                val matchingProbeIdxs = IntArrayList()

                repeat(probeSide.rowCount) { probeIdx ->
                    probeSide.forEachMatch(probeIdx) { buildIdx ->
                        matchingBuildIdxs.add(buildIdx)
                        matchingProbeIdxs.add(probeIdx)
                    }
                }

                return RelationReader.concatCols(
                    probeSide.buildRel.select(matchingBuildIdxs.toArray()),
                    probeSide.probeRel.select(matchingProbeIdxs.toArray())
                )
            }
        }

        @JvmField
        val LEFT_OUTER = Outer(LEFT)

        @JvmField
        val LEFT_OUTER_FLIPPED = Outer(LEFT_FLIPPED)

        @JvmField
        val FULL_OUTER = Outer(FULL)

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
                forEachIndexOf( {probeIdx, buildIdx -> if (buildIdx >= 0) sel.add(probeIdx) } ,false)
                sel.toArray()
            }

        @JvmField
        val SEMI = object : JoinType {
            override val joinTypeName = "semi-join"
            
            override fun probe(probeSide: ProbeSide): RelationReader {
                return probeSide.probeRel.select(probeSide.semiJoinSelection())
            }
        }

        internal fun ProbeSide.antiJoinSelection() =
            IntArrayList().let { sel ->
                repeat(rowCount) { if (matches(it) < 0) sel.add(it) }

                sel.toArray()
            }

        @JvmField
        val ANTI = object : JoinType {
            override val joinTypeName = "anti-join"
            
            override fun probe(probeSide: ProbeSide): RelationReader {
                return probeSide.probeRel.select(probeSide.antiJoinSelection())
            }
        }

        @JvmField
        val SINGLE = object : JoinType {
            override val joinTypeName = "single-join"
            
            override fun probe(probeSide: ProbeSide): RelationReader {
                val matchingBuildIdxs = IntArrayList()
                val matchingProbeIdxs = IntArrayList()

                repeat(probeSide.rowCount) { probeIdx ->
                    var matched = false
                    probeSide.forEachMatch(probeIdx) { buildIdx ->
                        if (matched)
                            throw Incorrect("cardinality violation", "xtdb.single-join/cardinality-violation")
                        matched = true

                        matchingBuildIdxs.add(buildIdx)
                        matchingProbeIdxs.add(probeIdx)
                    }

                    if (!matched) {
                        matchingBuildIdxs.add(probeSide.nullRowIdx)
                        matchingProbeIdxs.add(probeIdx)
                    }
                }

                return RelationReader.concatCols(
                    probeSide.buildRel.select(matchingBuildIdxs.toArray()),
                    probeSide.probeRel.select(matchingProbeIdxs.toArray())
                )
            }
        }
    }
}
