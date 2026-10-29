package xtdb.metadata

import com.carrotsearch.hppc.IntArrayList
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

    private fun writeNumericMetadata(flavours: List<Pair<Numeric, VectorMask>>, type: KClass<out Numeric>) {
        if (flavours.isNotEmpty()) {
            val typeVec = colsVec.vectorFor(type.java.metaColName, STRUCT_TYPE, true)
            val minVec = typeVec.vectorFor("min", F64_TYPE, false)
            val maxVec = typeVec.vectorFor("max", F64_TYPE, false)

            val minMax = DoubleMinMax()
            for ((flavour, mask) in flavours) flavour.minMaxInto(minMax, mask)

            if (minMax.min != Double.POSITIVE_INFINITY) minVec.writeDouble(minMax.min)
            if (minMax.max != Double.NEGATIVE_INFINITY) maxVec.writeDouble(minMax.max)
            typeVec.endStruct()
        }
    }

    private fun writeBytesMetadata(flavours: List<Pair<Bytes, VectorMask>>, calculateBlooms: Boolean) {
        if (flavours.isNotEmpty()) {
            val typeVec = colsVec.vectorFor(Bytes::class.java.metaColName, STRUCT_TYPE, false)
            val bloomVec = typeVec.vectorFor("bloom", VAR_BINARY_TYPE, true)

            if (calculateBlooms) {
                val bloomBuilder = BloomBuilder()
                for ((flavour, mask) in flavours)
                    repeat(flavour.valueCount) { if (mask.isSet(it)) bloomBuilder.add(flavour, it) }
                bloomVec.writeBytes(bloomBuilder.build().toByteBuffer())
            }

            typeVec.endStruct()
        }
    }

    private fun writePresenceMetadata(flavours: List<Presence>) {
        flavours.forEach {
            colsVec.vectorFor(it.arrowType.toLeg(), BOOL_TYPE, true).writeBoolean(true)
        }
    }

    private fun writeMetadata(col: VectorReader, rootCol: Boolean, mask: VectorMask): RowIndex {
        val flavours = col.metadataFlavours(mask)

        val childIdxs = IntArrayList()

        for ((flavour, flavourMask) in flavours) {
            when (flavour) {
                is MetadataFlavour.List -> childIdxs.add(writeMetadata(flavour.listElements, false, VectorMask.ALL))
                is MetadataFlavour.Set -> childIdxs.add(writeMetadata(flavour.listElements, false, VectorMask.ALL))
                is Struct -> {
                    val childMask =
                        if (!flavour.nullable) flavourMask
                        else object : VectorMask {
                            override fun isSet(idx: Int) = flavourMask.isSet(idx) && !flavour.isNull(idx)
                        }
                    flavour.vectors.forEach { childIdxs.add(writeMetadata(it, false, childMask)) }
                }
                else -> Unit
            }
        }

        var childIdx = 0

        val bytes = mutableListOf<Pair<Bytes, VectorMask>>()
        val dateTimes = mutableListOf<Pair<Numeric, VectorMask>>()
        val durations = mutableListOf<Pair<Numeric, VectorMask>>()
        val numbers = mutableListOf<Pair<Numeric, VectorMask>>()
        val times = mutableListOf<Pair<Numeric, VectorMask>>()
        val presence = mutableListOf<Presence>()

        for ((flavour, flavourMask) in flavours) {
            when (flavour) {
                is Bytes -> bytes.add(flavour to flavourMask)

                is MetadataFlavour.List ->
                    colsVec.vectorFor("list", I32_TYPE, true)
                        .writeInt(childIdxs[childIdx++])

                is MetadataFlavour.Set ->
                    colsVec.vectorFor("set", I32_TYPE, true)
                        .writeInt(childIdxs[childIdx++])

                is DateTime -> dateTimes.add(flavour to flavourMask)
                is Duration -> durations.add(flavour to flavourMask)
                is MetadataFlavour.Number -> numbers.add(flavour to flavourMask)
                is TimeOfDay -> times.add(flavour to flavourMask)

                is Struct -> {
                    val keysVec = colsVec.vectorFor("struct", LIST_TYPE, true)
                    val keyVec = keysVec.getListElements(I32_TYPE, false)

                    repeat(flavour.vectors.count()) { keyVec.writeInt(childIdxs[childIdx++]) }

                    keysVec.endList()
                }

                is Presence -> presence.add(flavour)
            }
        }

        assert(childIdx == childIdxs.size()) { "haven't used up all the nested vectors" }

        colNameVec.writeObject(col.name)
        rootColVec.writeBoolean(rootCol)
        countVec.writeLong((0 until col.valueCount).count { mask.isSet(it) && !col.isNull(it) }.toLong())

        writeNumericMetadata(numbers, MetadataFlavour.Number::class)
        writeNumericMetadata(dateTimes, DateTime::class)
        writeNumericMetadata(times, TimeOfDay::class)
        writeNumericMetadata(durations, Duration::class)

        writeBytesMetadata(bytes, calculateBlooms = calculateBlooms || (col.name == "_iid" && rootCol))

        writePresenceMetadata(presence)

        colsVec.endStruct()

        return colsVec.valueCount - 1
    }

    fun writeMetadata(col: VectorReader) = writeMetadata(col, true, VectorMask.ALL)
}