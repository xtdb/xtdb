package xtdb.operator.join

import org.apache.arrow.vector.types.pojo.Field
import xtdb.ICursor
import xtdb.arrow.RelationReader
import xtdb.operator.join.ComparatorFactory.Companion.build
import xtdb.types.FieldName
import java.util.function.Consumer

class MemoryHashJoin(
    private val buildSide: BuildSide, private val probeCursor: ICursor,
    private val probeFields: List<Field>?, private val probeKeyColNames: List<FieldName>,
    private val joinType: JoinType, private val comparatorFactory: ComparatorFactory,
) : ICursor {

    override val cursorType: String get() = "memory-hash-join"
    override val childCursors: List<ICursor> = listOf(probeCursor)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        var advanced = false

        while (!advanced) {
            probeCursor.tryAdvance { probeRel ->
                val comparator = comparatorFactory.build(buildSide, probeRel, probeKeyColNames)
                val probeSide = ProbeSide(buildSide, probeRel, probeKeyColNames, comparator)
                val outRel = joinType.probe(probeSide)
                if (outRel.rowCount > 0) {
                    c.accept(outRel)
                    advanced = true
                }

            } || break
        }

        if (advanced) return true

        if (probeFields == null) return false // semi-join
        val unmatchedBuildIdxsRel = buildSide.unmatchedIdxsRel(probeFields.map { it.name }, joinType) ?: return false
        buildSide.clearMatches() // so that we don't keep yielding them
        c.accept(unmatchedBuildIdxsRel)

        return true
    }

    override fun close() {
        probeCursor.close()
        // we don't own buildSide
    }
}