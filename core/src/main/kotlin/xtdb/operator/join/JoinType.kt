package xtdb.operator.join

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
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

    /**
     * Every probe row, plus its three-valued mark: TRUE where some build row passes `:conditions` and the mark,
     * NULL where none does but the mark is unknown for one, FALSE otherwise.
     *
     * The returned relation's mark column is reused by the next call, and freed by [close].
     */
    class Mark(al: BufferAllocator, markColName: String, private val spec: Spec) : JoinType, AutoCloseable {

        /** What decides a probe row's mark, over the build rows that pass `:conditions`. */
        sealed interface Spec {
            /** EXISTS: any such row marks TRUE. */
            data object Exists : Spec

            /** The three-valued OR of a comparator over those rows, built by [cmpFactory] over the loaded build rows and a probe relation. */
            class Compare(val cmpFactory: BiFunction<RelationReader, RelationReader, IntBinaryOperator>) : Spec
        }

        override val joinTypeName = "mark-join"

        private val markCol = BitVector(al, markColName, true)

        private inline fun withMarkCol(probeRel: RelationReader, mark: (Int) -> Int): RelationReader {
            markCol.clear()
            markCol.ensureCapacity(probeRel.rowCount)

            repeat(probeRel.rowCount) { probeIdx ->
                when (mark(probeIdx)) {
                    1 -> markCol.setBoolean(probeIdx, true)
                    0 -> markCol.setNull(probeIdx)
                    else -> markCol.setBoolean(probeIdx, false)
                }
            }

            return RelationReader.from(probeRel.vectors + markCol, probeRel.rowCount)
        }

        private fun ProbeSide.markOver(buildIdxs: IntIterator, probeIdx: Int, markCmp: IntBinaryOperator?): Int {
            var acc = -1
            while (buildIdxs.hasNext()) {
                val buildIdx = buildIdxs.nextInt()
                val res = if (!passes(buildIdx, probeIdx)) -1 else markCmp?.applyAsInt(buildIdx, probeIdx) ?: 1
                if (res == 1) return 1
                acc = maxOf(acc, res)
            }
            return acc
        }

        override fun probe(probeSide: ProbeSide): RelationReader {
            val markCmp = (spec as? Spec.Compare)?.cmpFactory?.apply(probeSide.buildRel, probeSide.probeRel)
            val keyCols = probeSide.keyColNames.map { probeSide.probeRel[it] }

            return withMarkCol(probeSide.probeRel) { probeIdx ->
                // the keys are two-valued, so a NULL one can't pass: its bucket holds every NULL-keyed build row, unwalked
                val keyIsNull = keyCols.any { it.isNull(probeIdx) }
                if (keyIsNull) -1 else probeSide.markOver(probeSide.candidates(probeIdx), probeIdx, markCmp)
            }
        }

        /**
         * Marks [probeRel] against every build row, partition by partition when spilled.
         * Used where hashing can't narrow the walk: a build side with no keys hashes every row alike.
         */
        fun probeEveryRow(
            buildSide: BuildSide, probeRel: RelationReader, probeKeyColNames: List<FieldName>,
            cmpFactory: ComparatorFactory,
        ): RelationReader {
            val markCmpFactory = when (spec) {
                Spec.Exists -> null
                is Spec.Compare -> spec.cmpFactory
            }

            val res = IntArray(probeRel.rowCount) { -1 }

            fun markAgainstLoadedRows() {
                val probeSide = ProbeSide(buildSide, probeRel, probeKeyColNames, cmpFactory.build(buildSide, probeRel, probeKeyColNames))
                val markCmp = markCmpFactory?.apply(buildSide.dataRel, probeRel)
                val buildRowCount = buildSide.dataRel.rowCount
                repeat(probeSide.rowCount) { probeIdx ->
                    if (res[probeIdx] != 1)
                        res[probeIdx] = maxOf(res[probeIdx], probeSide.markOver((0 until buildRowCount).iterator(), probeIdx, markCmp))
                }
            }

            if (buildSide.shuffle == null) markAgainstLoadedRows()
            else repeat(buildSide.partCount) { partIdx -> buildSide.loadPart(partIdx); markAgainstLoadedRows() }

            return withMarkCol(probeRel) { res[it] }
        }

        override fun close() = markCol.close()
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
                repeat(rowCount) { if (matches(it) != 1) sel.add(it) }

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
