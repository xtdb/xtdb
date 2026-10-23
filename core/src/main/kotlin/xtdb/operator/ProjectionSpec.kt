package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.IndirectRelation
import xtdb.arrow.LongVector
import xtdb.arrow.RelationAsStructReader
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.arrow.FieldName
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.util.closeOnCatch

interface ProjectionSpec {
    val toName: FieldName
    val type: VectorType
    val field: Field get() = type.toField(toName)

    /**
     * @param args a single-row indirect relation containing the args for this invocation - maybe a view over a bigger arg relation.
     */
    fun project(
        allocator: BufferAllocator,
        inRel: RelationReader,
        schema: Map<String, Any>,
        args: RelationReader
    ): VectorReader

    /**
     * Projects the rows `inner[sel[i]]`, as a dense vector of `sel.size` rows - the same as [project] over
     * `IndirectRelation(inner, sel)`, but implementations may read [inner] at `sel[i]` directly.
     */
    fun projectSel(
        allocator: BufferAllocator, inner: RelationReader, sel: IntArray,
        schema: Map<String, Any>, args: RelationReader
    ): VectorReader = project(allocator, IndirectRelation(inner, sel), schema, args)

    class Identity(override val toName: FieldName, override val type: VectorType) : ProjectionSpec {
        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ): VectorReader = inRel[toName]
    }

    class RowNumber(override val toName: FieldName) : ProjectionSpec {
        override val type: VectorType = VectorType.I64

        private var rowNum = 1L

        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) =
            LongVector(allocator, toName, false).closeOnCatch { rowNumVec ->
                repeat(inRel.rowCount) { rowNumVec.writeLong(rowNum++) }
                rowNumVec
            }
    }

    // only returns the row number within the batch - see #4131
    class LocalRowNumber(override val toName: FieldName) : ProjectionSpec {
        override val type: VectorType = VectorType.I64

        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ): VectorReader =
            LongVector(allocator, toName, false).closeOnCatch { rowNumVec ->
                var rowNum = 1L
                repeat(inRel.rowCount) { rowNumVec.writeLong(rowNum++) }
                rowNumVec
            }
    }

    class Star(override val toName: FieldName, override val type: VectorType) : ProjectionSpec {
        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) = RelationAsStructReader(toName, inRel).openSlice(allocator)
    }

    class Rename(private val fromName: FieldName, override val toName: FieldName, override val type: VectorType) : ProjectionSpec {
        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) =
            inRel.vectorFor(fromName).withName(toName)
    }
}
