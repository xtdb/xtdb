package xtdb.operator.join

import xtdb.api.ICursor
import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import java.util.function.Consumer

class EveryRowMarkJoin(
    private val buildSide: BuildSide, private val probeCursor: ICursor,
    private val probeKeyColNames: List<FieldName>,
    private val mark: JoinType.Mark, private val comparatorFactory: ComparatorFactory,
) : ICursor {

    override val cursorType: String get() = "every-row-mark-join"
    override val childCursors: List<ICursor> = listOf(probeCursor)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        var advanced = false

        while (!advanced) {
            probeCursor.tryAdvance { probeRel ->
                if (probeRel.rowCount > 0) {
                    c.accept(mark.probeEveryRow(buildSide, probeRel, probeKeyColNames, comparatorFactory))
                    advanced = true
                }
            } || break
        }

        return advanced
    }

    override fun close() {
        probeCursor.close()
        // we don't own buildSide or mark
    }
}
