package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.hash.MurmurHasher
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.IntVector
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.expression.map.IndexHasher
import xtdb.expression.map.RelationMapProber
import xtdb.trie.MutableMemoryHashTrie
import xtdb.util.Hasher
import xtdb.vector.OldRelationWriter
import java.util.function.IntBinaryOperator
import java.util.function.IntConsumer

class JoinRelationMap(
    val allocator: BufferAllocator,
    val schema: Schema,
    val buildKeyColumnNames: List<String>,
    val probeKeyColumnNames: List<String>,
    private val comparatorFactory: ComparatorFactory,
    withNilRow: Boolean = false,
    pageLimit: Int = 16, levelBits: Int = 2
) : AutoCloseable {

    interface ComparatorFactory {
        fun buildEqui(buildCol: VectorReader, probeCol: VectorReader): IntBinaryOperator
        fun buildTheta(buildRel: RelationReader, probeRel: RelationReader): IntBinaryOperator?
    }

    private val relWriter = OldRelationWriter(allocator, schema)
    private val buildKeyCols = buildKeyColumnNames.map { relWriter[it].asReader }

    private val hashColumn: IntVector = IntVector(allocator, "xt/join-hash", false)
    private var buildHashTrie: MutableMemoryHashTrie

    init {
        if (withNilRow) {
            relWriter.endRow()
            hashColumn.writeInt(0)
        }

        buildHashTrie =
            MutableMemoryHashTrie.builder(hashColumn.asReader).setPageLimit(pageLimit).setLevelBits(levelBits).build()
    }


    private fun andIBO(p1: IntBinaryOperator, p2: IntBinaryOperator) = IntBinaryOperator { l, r ->
        val lRes = p1.applyAsInt(l, r)
        if (lRes == -1) -1 else minOf(lRes, p2.applyAsInt(l, r))
    }

    private fun createHasher(cols: List<VectorReader>): IndexHasher {
        val hasher = Hasher.Xx()
        return IndexHasher { index ->
            cols.foldRight(0) { col, acc ->
                MurmurHasher.combineHashCode(
                    acc,
                    col.hashCode(index, hasher)
                )
            }
        }
    }

    @Suppress("NAME_SHADOWING")
    fun append(inRel: RelationReader) {
        val inKeyCols = buildKeyColumnNames.map { inRel.vectorForOrNull(it) as VectorReader }

        val hasher = createHasher(inKeyCols)
        val rowCopier = relWriter.rowCopier(inRel)

        repeat(inRel.rowCount) { inIdx ->
            // outIndex and used index for hashColumn should be identical
            val outIdxHashColumn = hashColumn.valueCount
            hashColumn.writeInt(hasher.hashCode(inIdx))
            buildHashTrie += outIdxHashColumn

            val outIdx = rowCopier.copyRow(inIdx)
            assert(outIdx == outIdxHashColumn) {
                "Expected outIdx $outIdx to match hashColumn valueCount $outIdxHashColumn"
            }
        }
    }

    fun probeFromRelation(probeRel: RelationReader): RelationMapProber {
        val buildRel = getBuiltRelation()
        val probeKeyCols = probeKeyColumnNames.map { probeRel.vectorForOrNull(it) as VectorReader }

        val comparator = buildKeyCols.zip(probeKeyCols)
            .map { (buildCol, probeCol) -> comparatorFactory.buildEqui(buildCol, probeCol) }
            .plus(listOfNotNull(comparatorFactory.buildTheta(buildRel, probeRel)))
            .reduceOrNull(::andIBO)
            ?: IntBinaryOperator { _, _ -> 1 }

        val hasher = createHasher(probeKeyCols)

        return object : RelationMapProber {
            // TODO one could very likely do a similar thing to a merge sort phase with the buildHashTrie and a probeHashTrie
            override fun indexOf(probeIdx: Int, removeOnMatch: Boolean): Int {
                val hashCode = hasher.hashCode(probeIdx)
                return buildHashTrie.findValue(
                    hashCode,
                    { buildIdx -> comparator.applyAsInt(buildIdx, probeIdx) },
                    removeOnMatch
                )
            }

            override fun forEachMatch(probeIdx: Int, c: IntConsumer) {
                val hashCode = hasher.hashCode(probeIdx)
                val candidates = buildHashTrie.findCandidates(hashCode)
                for (i in 0 until candidates.size()) {
                    if (comparator.applyAsInt(candidates[i], probeIdx) == 1) {
                        c.accept(candidates[i])
                    }
                }
            }

            override fun matches(probeIdx: Int): Int {
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
    }

    fun getBuiltRelation(): RelationReader = relWriter.asReader

    override fun close() {
        relWriter.close()
        hashColumn.close()
    }
}
