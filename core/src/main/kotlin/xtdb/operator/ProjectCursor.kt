package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.ICursor
import xtdb.arrow.VectorReader
import xtdb.util.useAll
import xtdb.arrow.RelationReader
import java.util.function.Consumer

class ProjectCursor(
    private val al: BufferAllocator,
    private val inCursor: ICursor<RelationReader>,
    private val specs: List<ProjectionSpec>,
    private val schema: Map<String, Any>,
    private val args: RelationReader,
) : ICursor<RelationReader> {
    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean = inCursor.tryAdvance { inRel ->
        mutableListOf<VectorReader>().useAll { closeCols ->
            val outCols = specs.map { spec ->
                spec.project(al, inRel, schema, args)
                    .also {
                        if (spec !is ProjectionSpec.Identity && spec !is ProjectionSpec.Rename)
                            closeCols.add(it)
                    }
            }

            c.accept(RelationReader.from(outCols, inRel.rowCount))
        }
    }

    override fun close() = inCursor.close()
}