package xtdb.arrow.metadata

import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.UNION_TYPE
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType
import xtdb.util.Hasher
import java.nio.ByteBuffer

sealed interface MetadataFlavour {
    val valueCount: Int
    val nullable: Boolean
    val arrowType: ArrowType
    fun isNull(idx: Int): Boolean

    sealed interface Numeric : MetadataFlavour {
        fun getMetaDouble(idx: Int): Double
    }

    interface Presence : MetadataFlavour

    interface Number : Numeric
    interface DateTime : Numeric
    interface Duration : Numeric
    interface TimeOfDay : Numeric

    interface Bytes : MetadataFlavour {
        fun getBytes(idx: Int): ByteBuffer
        fun hashCode(idx: Int, hasher: Hasher): Int
    }

    interface List : MetadataFlavour {
        val listElements: VectorReader
    }

    interface Set : MetadataFlavour {
        val listElements: VectorReader
    }

    interface Struct : MetadataFlavour {
        val vectors: Iterable<VectorReader>
    }

    companion object {
        private fun ArrowType.unsupported(): Nothing =
            throw UnsupportedOperationException(this::class.java.simpleName)

        private val ArrowType.metadataFlavour
            get() = accept(object : ArrowType.ArrowTypeVisitor<Class<out MetadataFlavour>?> {

                override fun visit(type: ArrowType.Null) = Presence::class.java

                override fun visit(type: ArrowType.Struct) = Struct::class.java

                override fun visit(type: ArrowType.List) = List::class.java
                override fun visit(type: ArrowType.LargeList) = type.unsupported()
                override fun visit(type: ArrowType.FixedSizeList) = List::class.java
                override fun visit(type: ArrowType.ListView) = type.unsupported()
                override fun visit(type: ArrowType.LargeListView) = type.unsupported()

                override fun visit(type: ArrowType.Union) = error("Union should have already been handled")
                override fun visit(type: ArrowType.Map) = type.unsupported()

                override fun visit(type: ArrowType.Int) = Number::class.java
                override fun visit(type: ArrowType.FloatingPoint) = Number::class.java

                override fun visit(type: ArrowType.Utf8) = Bytes::class.java
                override fun visit(type: ArrowType.Utf8View) = type.unsupported()
                override fun visit(type: ArrowType.LargeUtf8) = type.unsupported()

                override fun visit(type: ArrowType.Binary) = Bytes::class.java
                override fun visit(type: ArrowType.BinaryView) = type.unsupported()
                override fun visit(type: ArrowType.LargeBinary) = type.unsupported()
                override fun visit(type: ArrowType.FixedSizeBinary) = Bytes::class.java

                override fun visit(type: ArrowType.Bool) = Presence::class.java
                override fun visit(type: ArrowType.Decimal) = type.unsupported()
                override fun visit(type: ArrowType.Date) = DateTime::class.java
                override fun visit(type: ArrowType.Time) = TimeOfDay::class.java
                override fun visit(type: ArrowType.Timestamp) = DateTime::class.java
                override fun visit(type: ArrowType.Interval) = Presence::class.java
                override fun visit(type: ArrowType.Duration) = Duration::class.java

                override fun visit(type: ArrowType.RunEndEncoded) = type.unsupported()
            })

        @JvmStatic
        val VectorType.metadataFlavour: Class<out MetadataFlavour>?
            get() = when(this) {
                is VectorType.Mono -> arrowType.metadataFlavour
                is VectorType.Maybe -> mono.metadataFlavour
                is VectorType.Poly -> UNION_TYPE.unsupported()
            }

        @JvmStatic
        val Class<out MetadataFlavour>.metaColName
            get() = when (this) {
                Number::class.java -> "numbers"
                DateTime::class.java -> "date-times"
                TimeOfDay::class.java -> "times-of-day"
                Duration::class.java -> "durations"
                Bytes::class.java -> "bytes"
                Struct::class.java -> "structs"
                List::class.java -> "lists"
                Set::class.java -> "sets"
                Presence::class.java -> "other"

                else -> error("unknown metadata flavour")
            }
    }
}