package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.LongVector
import xtdb.arrow.RelationReader
import xtdb.arrow.openAsStruct
import xtdb.arrow.VectorReader
import xtdb.arrow.FieldName
import xtdb.arrow.VectorType
import xtdb.util.closeOnCatch

interface ProjectionSpec : AutoCloseable {
    interface Factory {
        val type: VectorType

        /**
         * @param args a single-row indirect relation containing the args for this invocation - maybe a view over a bigger arg relation.
         */
        fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader): ProjectionSpec
    }

    fun project(inRel: RelationReader): VectorReader

    class Identity(private val fromName: FieldName, override val type: VectorType) : Factory, ProjectionSpec {
        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) = this

        override fun project(inRel: RelationReader): VectorReader = inRel.vectorFor(fromName)

        override fun close() = Unit
    }

    object RowNumber : Factory {
        override val type: VectorType = VectorType.I64

        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) =
            object : ProjectionSpec {
                private var rowNum = 1L

                override fun project(inRel: RelationReader) =
                    LongVector(al, false).closeOnCatch { rowNumVec ->
                        repeat(inRel.rowCount) { rowNumVec.writeLong(rowNum++) }
                        rowNumVec
                    }

                override fun close() = Unit
            }
    }

    // only returns the row number within the batch - see #4131
    object LocalRowNumber : Factory {
        override val type: VectorType = VectorType.I64

        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) =
            object : ProjectionSpec {
                override fun project(inRel: RelationReader): VectorReader =
                    LongVector(al, false).closeOnCatch { rowNumVec ->
                        var rowNum = 1L
                        repeat(inRel.rowCount) { rowNumVec.writeLong(rowNum++) }
                        rowNumVec
                    }

                override fun close() = Unit
            }
    }

    class Star(override val type: VectorType) : Factory {
        override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader) =
            object : ProjectionSpec {
                override fun project(inRel: RelationReader) = inRel.openAsStruct(al)

                override fun close() = Unit
            }
    }
}
