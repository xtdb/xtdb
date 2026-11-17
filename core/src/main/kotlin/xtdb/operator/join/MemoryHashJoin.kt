package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.RelationReader
import xtdb.arrow.FieldName
import java.util.function.Consumer

class MemoryHashJoin(
    al: BufferAllocator, private val buildSide: BuildSide, private val probeCursor: ICursor,
    private val probeColNames: List<FieldName>, probeKeyColNames: List<FieldName>,
    private val joinType: JoinType, comparatorFactory: ComparatorFactory,
) : ICursor {

    private val probeSide = ProbeSide(al, buildSide, probeKeyColNames, comparatorFactory)

    override val cursorType: String get() = "memory-hash-join"
    override val childCursors: List<ICursor> = listOf(probeCursor)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        var advanced = false

        while (!advanced) {
            probeCursor.tryAdvance { probeRel ->
                val outRel = joinType.probe(probeSide.forBatch(probeRel))
                if (outRel.rowCount > 0) {
                    c.accept(outRel)
                    advanced = true
                }

            } || break
        }

        if (advanced) return true

        val unmatchedBuildIdxsRel = buildSide.unmatchedIdxsRel(probeColNames, joinType) ?: return false
        buildSide.clearMatches() // so that we don't keep yielding them
        c.accept(unmatchedBuildIdxsRel)

        return true
    }

    override fun close() {
        probeSide.close()
        probeCursor.close()
        // we don't own buildSide
    }
}