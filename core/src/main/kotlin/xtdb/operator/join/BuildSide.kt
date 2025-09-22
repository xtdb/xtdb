package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.types.pojo.Schema
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorReader
import xtdb.expression.map.IndexHasher.Companion.hasher
import xtdb.types.FieldName
import xtdb.types.Type.Companion.I32
import xtdb.types.Type.Companion.ofType
import xtdb.vector.ValueVectorReader.from
import java.util.function.IntConsumer
import java.util.function.IntUnaryOperator

class BuildSide(
    private val al: BufferAllocator,
    val schema: Schema,
    val keyColNames: List<String>,
    trackUnmatchedIdxs: Boolean,
    val withNilRow: Boolean,
    private val inMemoryThreshold: Long = 100_000,
) : AutoCloseable {
    private val dataRel = Relation(al, schema)
    private val hashCol = "hashes".ofType(I32).openVector(al)

    private var builtDataRel: RelationReader? = null
    val builtRel get() = builtDataRel!!
    var buildMap: BuildSideMap? = null; private set

    internal var spill: Spill? = null; private set

    private fun openSpill(): Spill = Spill.open(al, dataRel).also { this.spill = it }

    @Suppress("NAME_SHADOWING")
    fun append(inRel: RelationReader) {
        inRel.openDirectSlice(al).use { inRel ->
            val rowCopier = inRel.rowCopier(dataRel)

            repeat(inRel.rowCount) { inIdx -> rowCopier.copyRow(inIdx) }

            if (dataRel.rowCount > inMemoryThreshold)
                (this.spill ?: openSpill()).spill()
        }
    }

    var shuffle: Shuffle? = null; private set

    private fun buildRel(dataRel: Relation, hashCol: VectorReader) {
        unmatchedBuildIdxs?.let {
            it.clear()
            it.add(0L, dataRel.rowCount.toLong())
        }

        if (withNilRow) dataRel.endRow()
        builtDataRel?.close()
        builtDataRel = RelationReader.from(dataRel.openAsRoot(al))

        buildMap?.close()
        buildMap = BuildSideMap.from(al, hashCol)
    }

    fun end() {
        val spill = this.spill

        if (spill != null) {
            spill.spill()
            spill.end()

            val shuffle = Shuffle.open(al, dataRel, keyColNames, spill.rowCount, spill.blockCount)
                .also { this.shuffle = it }

            spill.openDataLoader().use { dataLoader ->
                while (dataLoader.loadNextPage(dataRel)) {
                    shuffle.shuffle()
                }
            }
            shuffle.end()

            dataRel.clear()
            builtDataRel?.close()
            builtDataRel = RelationReader.from(dataRel.openAsRoot(al))
        } else {
            dataRel.hasher(keyColNames).writeAllHashes(hashCol)
            buildRel(dataRel, hashCol)
        }
    }


    val partCount: Int get() = shuffle?.partCount ?: 0

    fun loadPart(partIdx: Int) {
        val shuffle = checkNotNull(shuffle) { "no spilled data" }

        shuffle.loadDataPart(dataRel, partIdx)
        shuffle.loadHashPart(hashCol, partIdx)

        buildRel(dataRel, hashCol)
    }


    val nullRowIdx: Int
        get() {
            check(this.withNilRow) { "no nil row in build side" }
            return builtRel.rowCount - 1
        }

    private val unmatchedBuildIdxs = if (trackUnmatchedIdxs) RoaringBitmap() else null

    fun addMatch(idx: Int) = unmatchedBuildIdxs?.remove(idx)
    fun clearMatches() = unmatchedBuildIdxs?.clear()

    fun unmatchedIdxsRel(nullColNames: List<FieldName>, joinType: JoinType): RelationReader? =
        unmatchedBuildIdxs
            ?.takeIf { !it.isEmpty }?.toArray()
            ?.let { idxs ->
                val buildRel = builtRel.select(idxs)
                val probeRel =
                    RelationReader
                        .from(nullColNames.map { from(NullVector(it, 1)) })
                        .select(IntArray(idxs.size))

                if (joinType.outerJoinType == JoinType.OuterJoinType.LEFT_FLIPPED)
                    RelationReader.concatCols(probeRel, buildRel)
                else
                    RelationReader.concatCols(buildRel, probeRel)
            }

    fun indexOf(hashCode: Int, cmp: IntUnaryOperator, removeOnMatch: Boolean): Int =
        requireNotNull(buildMap).findValue(hashCode, cmp, removeOnMatch)

    fun forEachMatch(hashCode: Int, c: IntConsumer) =
        requireNotNull(buildMap).forEachMatch(hashCode, c)

    override fun close() {
        buildMap?.close()
        builtDataRel?.close()

        shuffle?.close()
        spill?.close()

        hashCol.close()
        dataRel.close()
    }
}
