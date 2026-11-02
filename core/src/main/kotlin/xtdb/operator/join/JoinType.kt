package xtdb.operator.join

import com.carrotsearch.hppc.IntArrayList
import xtdb.arrow.BitVector
import xtdb.arrow.RelationReader
import xtdb.api.error.Incorrect
import xtdb.arrow.FieldName
import xtdb.operator.join.ComparatorFactory.Companion.build
import xtdb.operator.join.JoinType.OuterJoinType.*
import java.util.function.BiFunction
import java.util.function.IntBinaryOperator

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

                for (buildIdx in probeSide.iterator(probeIdx)) {
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
                    for (buildIdx in probeSide.iterator(probeIdx)) {
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

        /**
         * @param markCmpFactory builds the `:mark-spec` comparator over the loaded build rows and [probeRel];
         *   null where the mark is constant `true` (EXISTS), so a row is marked TRUE wherever `:conditions` match.
         */
        @JvmStatic
        fun BuildSide.mark(
            probeRel: RelationReader, probeKeyColNames: List<FieldName>,
            cmpFactory: ComparatorFactory,
            markCmpFactory: BiFunction<RelationReader, RelationReader, IntBinaryOperator>?,
            markCol: BitVector
        ) {
            val res = IntArray(probeRel.rowCount) { -1 }

            fun markAgainstLoadedRows() {
                val conditionsCmp = cmpFactory.build(this, probeRel, probeKeyColNames)
                val markCmp = markCmpFactory?.apply(dataRel, probeRel)
                val cmp = IntBinaryOperator { buildIdx, probeIdx ->
                    when {
                        conditionsCmp.applyAsInt(buildIdx, probeIdx) != 1 -> -1
                        markCmp == null -> 1
                        else -> markCmp.applyAsInt(buildIdx, probeIdx)
                    }
                }
                val probeSide = ProbeSide(this, probeRel, probeKeyColNames, cmp)
                repeat(probeSide.rowCount) { probeIdx ->
                    if (res[probeIdx] != 1) res[probeIdx] = maxOf(res[probeIdx], probeSide.matches(probeIdx))
                }
            }

            if (shuffle == null) markAgainstLoadedRows()
            else repeat(partCount) { partIdx -> loadPart(partIdx); markAgainstLoadedRows() }

            res.forEachIndexed { probeIdx, r ->
                when (r) {
                    0 -> markCol.setNull(probeIdx)
                    1 -> markCol.setBoolean(probeIdx, true)
                    -1 -> markCol.setBoolean(probeIdx, false)
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
                    for (buildIdx in probeSide.iterator(probeIdx)) {
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
