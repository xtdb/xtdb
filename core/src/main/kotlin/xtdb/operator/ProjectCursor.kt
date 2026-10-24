package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.MaskedRelation
import xtdb.arrow.RelationReader
import xtdb.util.closeAll
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
            if (inRel is MaskedRelation && tryMasked(inRel, c)) return@tryAdvance

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

    // DART (#6285 avenue B): keep the mask through the projection if every spec can
    private fun tryMasked(inRel: MaskedRelation, c: Consumer<in RelationReader>): Boolean {
        val direct = mutableListOf<VectorReader>()
        val dense = mutableListOf<VectorReader>()
        val closeCols = mutableListOf<VectorReader>()
        try {
            for (spec in specs) {
                when (spec) {
                    is ProjectionSpec.Identity ->
                        if (inRel.isDirect(spec.toName)) direct.add(inRel.directVector(spec.toName))
                        else dense.add(inRel[spec.toName])

                    is ProjectionSpec.Rename ->
                        if (inRel.isDirect(spec.fromName)) direct.add(inRel.directVector(spec.fromName).withName(spec.toName))
                        else dense.add(inRel[spec.fromName].withName(spec.toName))

                    is MaskedProjection ->
                        dense.add((spec.projectMasked(al, inRel, schema, args) ?: return false).also { closeCols.add(it) })

                    else -> return false
                }
            }

            c.accept(inRel.withVectors(direct, dense))
            return true
        } finally {
            closeCols.closeAll()
        }
    }

    override fun close() = inCursor.close()
}