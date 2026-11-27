package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.FieldName
import xtdb.arrow.IntVector
import xtdb.arrow.RelationReader
import xtdb.expression.map.IndexHasher.Companion.hasher
import java.util.function.BiConsumer
import java.util.function.IntBinaryOperator

/**
 * The probe side of a hash join, for as long as its cursor: compares each probe batch against [buildSide] through a [ForBatch].
 */
class ProbeSide(
    private val al: BufferAllocator, private val buildSide: BuildSide,
    private val keyColNames: List<FieldName>, private val comparatorFactory: ComparatorFactory,
) : AutoCloseable {

    private val lazyHashes = lazy { IntVector.open(al, false) }
    private val lazyHashScratch = lazy { IntVector.open(al, false) }

    /**
     * Hashes [probeRel]'s keys into a vector this probe side owns.
     * The returned batch's hashes are valid until the next call to either `forBatch`.
     */
    fun forBatch(probeRel: RelationReader): ForBatch.Hashed {
        val hashes = lazyHashes.value
        probeRel.hasher(keyColNames).hashTo(hashes, lazyHashScratch.value)
        return forBatch(probeRel, hashes)
    }

    /** For a batch whose key hashes the caller already holds, row for row - [hashes] stays the caller's. */
    fun forBatch(probeRel: RelationReader, hashes: IntVector): ForBatch.Hashed =
        ForBatch.Hashed(buildSide, probeRel, keyColNames, comparatorFactory.build(buildSide, probeRel, keyColNames), hashes)

    override fun close() {
        if (lazyHashScratch.isInitialized()) lazyHashScratch.value.close()
        if (lazyHashes.isInitialized()) lazyHashes.value.close()
    }

    sealed class ForBatch(
        protected val buildSide: BuildSide, val probeRel: RelationReader,
        val keyColNames: List<FieldName>, protected val comparator: IntBinaryOperator,
    ) {
        internal val buildRel = buildSide.dataRel
        val nullRowIdx get() = buildSide.nullRowIdx
        val rowCount = probeRel.rowCount

        /** Whether the build and probe rows satisfy the join's conditions: an unknown result doesn't. */
        fun passes(buildIdx: Int, probeIdx: Int): Boolean = comparator.applyAsInt(buildIdx, probeIdx) == 1

        /** A batch compared against every build row, so its keys are never hashed. */
        class Unhashed(
            buildSide: BuildSide, probeRel: RelationReader,
            keyColNames: List<FieldName>, comparator: IntBinaryOperator,
        ) : ForBatch(buildSide, probeRel, keyColNames, comparator)

        /** A batch whose rows' key hashes are [hashes], so it looks up each row's candidates in the build side. */
        class Hashed(
            buildSide: BuildSide, probeRel: RelationReader,
            keyColNames: List<FieldName>, comparator: IntBinaryOperator,
            private val hashes: IntVector,
        ) : ForBatch(buildSide, probeRel, keyColNames, comparator) {

            fun indexOf(probeIdx: Int, removeOnMatch: Boolean): Int {
                val buildIdx = buildSide.indexOf(
                    hashes.getInt(probeIdx),
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
            fun candidates(probeIdx: Int): IntIterator = buildSide.iterator(hashes.getInt(probeIdx))
        }
    }
}
