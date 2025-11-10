package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.LongVector
import xtdb.arrow.RelationAsStructReader
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.trie.ColumnName
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.util.closeOnCatch

interface ProjectionSpec {
    val field: Field

    /**
     * @param args a single-row indirect relation containing the args for this invocation - maybe a view over a bigger arg relation.
     */
    fun project(
        allocator: BufferAllocator,
        inRel: RelationReader,
        schema: Map<String, Any>,
        args: RelationReader
    ): VectorReader

    class Identity(override val field: Field) : ProjectionSpec {
        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ): VectorReader = inRel[field.name]
    }

    class RowNumber(colName: ColumnName) : ProjectionSpec {
        override val field = colName ofType VectorType.I64

        private var rowNum = 1L

        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) =
            LongVector(allocator, field.name, false).closeOnCatch { rowNumVec ->
                repeat(inRel.rowCount) { rowNumVec.writeLong(rowNum++) }
                rowNumVec
            }
    }

    // only returns the row number within the batch - see #4131
    class LocalRowNumber(colName: ColumnName) : ProjectionSpec {
        override val field = colName ofType VectorType.I64

        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ): VectorReader =
            LongVector(allocator, field.name, false).closeOnCatch { rowNumVec ->
                var rowNum = 1L
                repeat(inRel.rowCount) { rowNumVec.writeLong(rowNum++) }
                rowNumVec
            }
    }

    class Star(override val field: Field) : ProjectionSpec {
        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) = RelationAsStructReader(field.name, inRel).openSlice(allocator)
    }

    class Rename(private val fromName: ColumnName, override val field: Field) : ProjectionSpec {
        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) =
            inRel.vectorFor(fromName).withName(field.name)
    }
}
