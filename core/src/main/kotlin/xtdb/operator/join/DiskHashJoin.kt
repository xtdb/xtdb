package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import xtdb.ICursor
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector.Companion.openVector
import xtdb.operator.join.ComparatorFactory.Companion.build
import xtdb.arrow.FieldName
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorTypes
import xtdb.util.closeOnCatch
import java.util.function.Consumer

class DiskHashJoin(
    private val al: BufferAllocator, private val buildSide: BuildSide,
    private val probeCursor: ICursor, private val probeKeyColNames: List<FieldName>, private val probeShuffle: Shuffle,
    private val joinType: JoinType, private val comparatorFactory: ComparatorFactory,
) : ICursor {
    override val cursorType = "disk-hash-join"
    override val childCursors = listOf(probeCursor)

    private fun probesPerBuild(buildParts: Int, probeParts: Int): Int {
        check(probeParts.mod(buildParts) == 0) {
            "probe parts ($probeParts) must be a multiple of build parts ($buildParts)"
        }

        check(probeParts >= buildParts) { "probe parts ($probeParts) must be >= build parts ($buildParts)" }

        return probeParts / buildParts
    }

    private val buildParts = buildSide.partCount
    private val probeParts = probeShuffle.partCount
    private val probesPerBuild = probesPerBuild(buildParts, probeParts)

    // we add one on here so that when we've finished all the matching probe partitions,
    // we can then yield the unmatched build rows (for outer joins)
    private val probesPerBuildPlusUnmatched = probesPerBuild + 1

    private var partIdx: Int = -1
    private val probeRel = Relation(al, probeShuffle.schema)
    private val probeColNames = probeRel.schema.fields.map { it.name }
    private val hashCol = al.openVector("hashes", I32)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        while (true) {
            if (++partIdx >= probesPerBuildPlusUnmatched * buildParts) return false

            val probePartIdxWithinBuildPart = partIdx.mod(probesPerBuildPlusUnmatched)

            if (probePartIdxWithinBuildPart == probesPerBuild) {
                buildSide.unmatchedIdxsRel(probeColNames, joinType)
                    ?.let { unmatchedIdxsRel ->
                        c.accept(unmatchedIdxsRel)
                        return true
                    }
            } else {
                val buildPartIdx = partIdx / probesPerBuildPlusUnmatched

                if (probePartIdxWithinBuildPart == 0)
                    buildSide.loadPart(buildPartIdx)

                val probePartIdx = probePartIdxWithinBuildPart * buildParts + buildPartIdx
                probeShuffle.loadDataPart(probeRel, probePartIdx)
                probeShuffle.loadHashPart(hashCol, probePartIdx)

                // TODO can probably try move this to a field now
                val probeSide = ProbeSide(
                    buildSide, probeRel, probeKeyColNames,
                    comparatorFactory.build(buildSide, probeRel, probeKeyColNames)
                )

                val joinedRel = joinType.probe(probeSide)

                if (joinedRel.rowCount > 0) {
                    c.accept(joinedRel)
                    return true
                }
            }
        }
    }

    override fun close() {
        hashCol.close()
        probeRel.close()
        probeShuffle.close()
        probeCursor.close()
        // we don't own buildSide
    }

    companion object {
        @JvmStatic
        fun open(
            al: BufferAllocator,
            buildSide: BuildSide, probeCursor: ICursor,
            probeVecTypes: VectorTypes, probeKeyColNames: List<FieldName>,
            joinType: JoinType, comparatorFactory: ComparatorFactory,
        ): DiskHashJoin =
            Relation(al, probeVecTypes).use { tmpRel ->
                Spill.open(al, tmpRel).use { spill ->
                    probeCursor.forEachRemaining { inRel ->
                        tmpRel.append(inRel)
                        if (tmpRel.rowCount > buildSide.inMemoryThreshold) spill.spill()
                    }

                    spill.end()
                    tmpRel.clear()

                    Shuffle.open(al, tmpRel, probeKeyColNames, spill.rowCount, spill.blockCount, buildSide.partCount)
                        .closeOnCatch { shuffle ->
                            spill.openDataLoader().use { spillLoader ->
                                while (spillLoader.loadNextPage(tmpRel))
                                    shuffle.shuffle()
                            }

                            shuffle.end()

                            DiskHashJoin(
                                al, buildSide,
                                probeCursor, probeKeyColNames,
                                shuffle, joinType, comparatorFactory
                            )
                        }
                }
            }
    }
}