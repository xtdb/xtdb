package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.IndirectRelation
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

    private val passThrough = { spec: ProjectionSpec -> spec is ProjectionSpec.Identity || spec is ProjectionSpec.Rename }

    // dart: `xtdb.projectMode` = sparse (default) | dense | off
    private val mode = System.getProperty("xtdb.projectMode", "sparse")

    private val denseOk = specs.none {
        it is ProjectionSpec.RowNumber || it is ProjectionSpec.LocalRowNumber || it is ProjectionSpec.Star
    }

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
        inCursor.tryAdvance { inRel ->
            mutableListOf<VectorReader>().useAll { closeCols ->
                val sel = IndirectRelation.selOf(inRel)
                val inner = IndirectRelation.innerOf(inRel)

                fun VectorReader.track(spec: ProjectionSpec) = also { if (!passThrough(spec)) closeCols.add(it) }

                when {
                    sel == null || mode == "off" -> {
                        val outCols = specs.map { spec -> spec.project(al, inRel, schema, args).track(spec) }
                        c.accept(RelationReader.from(outCols, inRel.rowCount))
                    }

                    mode == "dense" && denseOk -> {
                        val outCols = specs.map { spec -> spec.project(al, inner, schema, args).track(spec) }
                        c.accept(IndirectRelation(RelationReader.from(outCols, inner.rowCount), sel))
                    }

                    else -> {
                        val outCols = specs.map { spec ->
                            if (passThrough(spec)) spec.project(al, inRel, schema, args)
                            else spec.projectSel(al, inner, sel, schema, args).track(spec)
                        }
                        c.accept(RelationReader.from(outCols, inRel.rowCount))
                    }
                }
            }
        }

    override fun close() = inCursor.close()
}