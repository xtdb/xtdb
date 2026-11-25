package xtdb.operator

import xtdb.api.ICursor
import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.util.closeAll
import xtdb.util.useAll
import java.util.SequencedMap
import java.util.function.Consumer

class ProjectCursor(
    private val inCursor: ICursor,
    private val specs: SequencedMap<FieldName, ProjectionSpec>,
) : ICursor {

    override val cursorType get() = "project"
    override val childCursors get() = listOf(inCursor)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
        inCursor.tryAdvance { inRel ->
            mutableListOf<VectorReader>().useAll { closeCols ->
                val outCols = specs.mapValuesTo(linkedMapOf()) { (_, spec) ->
                    spec.project(inRel).also { if (spec !is ProjectionSpec.Identity) closeCols.add(it) }
                }

                c.accept(RelationReader.from(outCols, inRel.rowCount))
            }
        }

    override fun close() = (specs.values + inCursor).closeAll()
}
