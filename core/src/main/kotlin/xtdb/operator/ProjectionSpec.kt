package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.trie.ColumnName
import xtdb.types.Fields
import xtdb.types.withName
import xtdb.util.closeOnCatch
import xtdb.vector.IVectorReader
import xtdb.vector.ValueVectorReader
import xtdb.vector.asReader
import xtdb.vector.writerFor

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
        override val field = Fields.I64.toArrowField(colName)

        private var rowNum = 1L

        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) =
            BigIntVector(field, allocator).closeOnCatch { rowNumVec ->
                val wtr = writerFor(rowNumVec)
                repeat(inRel.rowCount) { wtr.writeLong(rowNum++) }
                wtr.asReader
            }
    }

    class Star(override val field: Field) : ProjectionSpec {
        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) =
            Fields.Struct(inRel.vectors.map { it.field.withName(it.name) })
                .toArrowField(field.name)
                .createVector(allocator)
                .let { it as StructVector }
                .closeOnCatch { structVec ->
                    // can we quickly set all of these to 1?
                    repeat(inRel.rowCount) { structVec.setIndexDefined(it) }

                    inRel.vectors.forEach { inVec ->
                        (inVec as IVectorReader).copyTo(structVec.getChild(inVec.name))
                    }

                    structVec.valueCount = inRel.rowCount

                    ValueVectorReader.from(structVec)
                }
    }

    class Rename(private val fromName: ColumnName, override val field: Field) : ProjectionSpec {
        override fun project(
            allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader
        ) =
            inRel.vectorFor(fromName).withName(field.name)
    }
}
