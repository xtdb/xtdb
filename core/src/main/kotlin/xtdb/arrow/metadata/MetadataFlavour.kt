package xtdb.arrow.metadata

import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.IntVector
import xtdb.arrow.UNION_TYPE
import xtdb.arrow.VectorIndirection.Mask
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

        fun minMaxInto(minMax: DoubleMinMax, mask: Mask) =
            repeat(valueCount) { if (mask.isSet(it) && !isNull(it)) minMax.applyMinMax(getMetaDouble(it)) }
    }

    interface Presence : MetadataFlavour

    interface Number : Numeric {
        fun getAsDouble(idx: Int): Double

        override fun getMetaDouble(idx: Int) = getAsDouble(idx)
    }

    interface DateTime : Numeric
    interface Duration : Numeric
    interface TimeOfDay : Numeric

    interface Bytes : MetadataFlavour {
        fun getBytes(idx: Int): ByteBuffer
        fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?)
    }

    interface List : MetadataFlavour {
        val listElements: VectorReader
    }

    interface Set : MetadataFlavour {
        val listElements: VectorReader
    }

    interface Struct : MetadataFlavour {
        val children: Map<String, VectorReader>

        /**
         * The mask over the rows of [children] that their metadata is taken under, given [mask] over this struct's rows:
         * the rows [mask] selects where this struct isn't null.
         */
        fun childMask(mask: Mask): Mask =
            if (!nullable) mask
            else Mask.Builder(mask.rowCount).also { rows ->
                for (idx in 0 until mask.rowCount) if (mask.isSet(idx) && !isNull(idx)) rows.add(idx)
            }.build()
    }

    companion object {
        internal fun MetadataFlavour.translatingRows(
            translateMask: (Mask) -> Mask, translateSel: (IntArray) -> IntArray
        ): MetadataFlavour {
            val f = this
            return when (f) {
                is Number -> object : Number by f {
                    override fun minMaxInto(minMax: DoubleMinMax, mask: Mask) = f.minMaxInto(minMax, translateMask(mask))
                }

                is DateTime -> object : DateTime by f {
                    override fun minMaxInto(minMax: DoubleMinMax, mask: Mask) = f.minMaxInto(minMax, translateMask(mask))
                }

                is Duration -> object : Duration by f {
                    override fun minMaxInto(minMax: DoubleMinMax, mask: Mask) = f.minMaxInto(minMax, translateMask(mask))
                }

                is TimeOfDay -> object : TimeOfDay by f {
                    override fun minMaxInto(minMax: DoubleMinMax, mask: Mask) = f.minMaxInto(minMax, translateMask(mask))
                }

                is Bytes -> object : Bytes by f {
                    override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?) =
                        f.hashTo(out, hasher, sel?.let(translateSel))
                }

                is Struct -> object : Struct by f {
                    override fun childMask(mask: Mask) = f.childMask(translateMask(mask))
                }

                is Presence, is List, is Set -> f
            }
        }

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
                VectorType.Nothing -> null // bottom: no values, hence no flavour
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