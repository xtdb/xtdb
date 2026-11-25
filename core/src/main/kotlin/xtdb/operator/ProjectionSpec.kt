package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.LongVector
import xtdb.arrow.RelationReader
import xtdb.arrow.openAsStruct
import xtdb.arrow.VectorReader
import xtdb.arrow.FieldName
import xtdb.arrow.VectorType
import xtdb.util.closeOnCatch

interface ProjectionSpec : AutoCloseable {
    interface Factory {
        val toName: FieldName
        val type: VectorType
        val field: Field get() = type.toField(toName)

        /**
         * @param args a single-row indirect relation containing the args for this invocation - maybe a view over a bigger arg relation.
         */
        fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader): ProjectionSpec
    }

    fun project(inRel: RelationReader): VectorReader

    class Identity(override val toName: FieldName, override val type: VectorType) : Factory, ProjectionSpec {
        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) = this

        override fun project(inRel: RelationReader): VectorReader = inRel[toName]

        override fun close() = Unit
    }

    class RowNumber(override val toName: FieldName) : Factory {
        override val type: VectorType = VectorType.I64

        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) =
            object : ProjectionSpec {
                private var rowNum = 1L

                override fun project(inRel: RelationReader) =
                    LongVector(al, toName, false).closeOnCatch { rowNumVec ->
                        repeat(inRel.rowCount) { rowNumVec.writeLong(rowNum++) }
                        rowNumVec
                    }

                override fun close() = Unit
            }
    }

    // only returns the row number within the batch - see #4131
    class LocalRowNumber(override val toName: FieldName) : Factory {
        override val type: VectorType = VectorType.I64

        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) =
            object : ProjectionSpec {
                override fun project(inRel: RelationReader): VectorReader =
                    LongVector(al, toName, false).closeOnCatch { rowNumVec ->
                        var rowNum = 1L
                        repeat(inRel.rowCount) { rowNumVec.writeLong(rowNum++) }
                        rowNumVec
                    }

                override fun close() = Unit
            }
    }

    class Star(override val toName: FieldName, override val type: VectorType) : Factory {
        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) =
            object : ProjectionSpec {
                override fun project(inRel: RelationReader) = inRel.openAsStruct(al, toName)

                override fun close() = Unit
            }
    }

    class Rename(
        private val fromName: FieldName, override val toName: FieldName, override val type: VectorType
    ) : Factory, ProjectionSpec {
        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) = this

        override fun project(inRel: RelationReader) = inRel.vectorFor(fromName).withName(toName)

        override fun close() = Unit
    }
}
