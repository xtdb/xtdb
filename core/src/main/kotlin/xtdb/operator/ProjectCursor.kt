package xtdb.operator

import xtdb.api.ICursor
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.util.closeAll
import xtdb.util.useAll
import java.util.function.Consumer

class ProjectCursor(
    private val inCursor: ICursor,
    private val specs: List<ProjectionSpec>,
) : ICursor {

    override val cursorType get() = "project"
    override val childCursors get() = listOf(inCursor)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
        inCursor.tryAdvance { inRel ->
            mutableListOf<VectorReader>().useAll { closeCols ->
                val outCols = specs.map { spec ->
                    spec.project(inRel)
                        .also {
                            if (spec !is ProjectionSpec.Identity && spec !is ProjectionSpec.Rename)
                                closeCols.add(it)
                        }
                }

                c.accept(RelationReader.from(outCols, inRel.rowCount))
            }
        }

    override fun close() = (specs + inCursor).closeAll()
}
