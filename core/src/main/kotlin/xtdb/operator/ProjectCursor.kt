package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.ICursor
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.util.useAll
import java.util.function.Consumer

class ProjectCursor(
    private val al: BufferAllocator,
    private val inCursor: ICursor,
    private val specs: List<ProjectionSpec>,
    private val schema: Map<String, Any>,
    private val args: RelationReader,
) : ICursor {

    override val cursorType get() = "project"
    override val childCursors get() = listOf(inCursor)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
        inCursor.tryAdvance { inRel ->
            inRel.openDirectSlice(al).use { inRel ->
                mutableListOf<VectorReader>().useAll { closeCols ->
                    val outCols = specs.map { spec ->
                        spec.project(al, inRel, schema, args)
                            .also {
                                if (spec !is ProjectionSpec.Identity && spec !is ProjectionSpec.Rename)
                                    closeCols.add(it)
                            }
                    }

                    RelationReader.from(outCols, inRel.rowCount).openDirectSlice(al).use { outRel ->
                        outRel.openAsRoot(al).use { root ->
                            c.accept(RelationReader.from(root))
                        }
                    }
                }
            }
        }

    override fun close() = inCursor.close()
}