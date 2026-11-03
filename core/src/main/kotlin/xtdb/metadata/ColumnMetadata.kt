package xtdb.metadata

import xtdb.arrow.BOOL_TYPE
import xtdb.arrow.F64_TYPE
import xtdb.arrow.I32_TYPE
import xtdb.arrow.LIST_TYPE
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VAR_BINARY_TYPE
import xtdb.arrow.VectorMask
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.BOOL
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.VAR_BINARY
import xtdb.arrow.VectorWriter
import xtdb.arrow.metadata.DoubleMinMax
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.arrow.metadata.MetadataFlavour.*
import xtdb.arrow.metadata.MetadataFlavour.Companion.metaColName
import xtdb.arrow.toLeg
import xtdb.bloom.BloomBuilder
import xtdb.bloom.toByteBuffer
import xtdb.trie.RowIndex
import kotlin.reflect.KClass

class ColumnMetadata(private val colsVec: VectorWriter, private val calculateBlooms: Boolean) {
    private val colNameVec = colsVec["col-name"]
    private val rootColVec = colsVec["root-col?"]
    private val countVec = colsVec["count"]

    private fun writeNumericMetadata(minMax: DoubleMinMax?, type: KClass<out Numeric>) {
        if (minMax != null) {
            val typeVec = colsVec.vectorFor(type.java.metaColName, STRUCT_TYPE, true)
            val minVec = typeVec.vectorFor("min", F64_TYPE, false)
            val maxVec = typeVec.vectorFor("max", F64_TYPE, false)

            if (minMax.min != Double.POSITIVE_INFINITY) minVec.writeDouble(minMax.min)
            if (minMax.max != Double.NEGATIVE_INFINITY) maxVec.writeDouble(minMax.max)
            typeVec.endStruct()
        }
    }

    private fun writeBytesMetadata(bloom: BloomBuilder?) {
        val typeVec = colsVec.vectorFor(Bytes::class.java.metaColName, STRUCT_TYPE, false)
        val bloomVec = typeVec.vectorFor("bloom", VAR_BINARY_TYPE, true)

        if (bloom != null) bloomVec.writeBytes(bloom.build().toByteBuffer())

        typeVec.endStruct()
    }

    private fun writeMetadata(col: VectorReader, rootCol: Boolean, mask: VectorMask): RowIndex {
        val withBloom = calculateBlooms || (col.name == "_iid" && rootCol)

        val childRefWrites = mutableListOf<() -> Unit>()
        var numbers: DoubleMinMax? = null
        var dateTimes: DoubleMinMax? = null
        var times: DoubleMinMax? = null
        var durations: DoubleMinMax? = null
        var bytesSeen = false
        var bloom: BloomBuilder? = null
        val presence = mutableListOf<Presence>()

        fun DoubleMinMax?.fold(flavour: Numeric) = (this ?: DoubleMinMax()).also { flavour.minMaxInto(it, mask) }

        for (flavour in col.metadataFlavours) {
            when (flavour) {
                is MetadataFlavour.List -> {
                    val childIdx = writeMetadata(flavour.listElements, false, VectorMask.ALL)
                    childRefWrites += { colsVec.vectorFor("list", I32_TYPE, true).writeInt(childIdx) }
                }

                is MetadataFlavour.Set -> {
                    val childIdx = writeMetadata(flavour.listElements, false, VectorMask.ALL)
                    childRefWrites += { colsVec.vectorFor("set", I32_TYPE, true).writeInt(childIdx) }
                }

                is Struct -> {
                    val childMask = flavour.childMask(mask)
                    val childIdxs = flavour.vectors.map { writeMetadata(it, false, childMask) }
                    childRefWrites += {
                        val keysVec = colsVec.vectorFor("struct", LIST_TYPE, true)
                        val keyVec = keysVec.getListElements(I32_TYPE, false)
                        childIdxs.forEach { keyVec.writeInt(it) }
                        keysVec.endList()
                    }
                }

                is MetadataFlavour.Number -> numbers = numbers.fold(flavour)
                is DateTime -> dateTimes = dateTimes.fold(flavour)
                is TimeOfDay -> times = times.fold(flavour)
                is Duration -> durations = durations.fold(flavour)

                is Bytes -> {
                    bytesSeen = true
                    if (withBloom) flavour.hashInto(bloom ?: BloomBuilder().also { bloom = it }, mask)
                }

                is Presence -> presence.add(flavour)
            }
        }

        childRefWrites.forEach { it() }

        colNameVec.writeObject(col.name)
        rootColVec.writeBoolean(rootCol)
        countVec.writeLong((0 until col.valueCount).count { mask.isSet(it) && !col.isNull(it) }.toLong())

        writeNumericMetadata(numbers, MetadataFlavour.Number::class)
        writeNumericMetadata(dateTimes, DateTime::class)
        writeNumericMetadata(times, TimeOfDay::class)
        writeNumericMetadata(durations, Duration::class)

        if (bytesSeen) writeBytesMetadata(bloom)

        presence.forEach { colsVec.vectorFor(it.arrowType.toLeg(), BOOL_TYPE, true).writeBoolean(true) }

        colsVec.endStruct()

        return colsVec.valueCount - 1
    }

    fun writeMetadata(col: VectorReader) = writeMetadata(col, true, VectorMask.ALL)
}