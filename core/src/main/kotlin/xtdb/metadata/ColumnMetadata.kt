package xtdb.metadata

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.BOOL_TYPE
import xtdb.arrow.F64_TYPE
import xtdb.arrow.I32_TYPE
import xtdb.arrow.IntVector
import xtdb.arrow.LIST_TYPE
import xtdb.arrow.RowMask
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VAR_BINARY_TYPE
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
import xtdb.bloom.RoaringSerializer
import xtdb.bloom.bloomPositions
import xtdb.trie.RowIndex
import xtdb.util.Hasher
import java.nio.ByteBuffer
import kotlin.reflect.KClass

class ColumnMetadata(
    private val al: BufferAllocator, private val colsVec: VectorWriter, private val calculateBlooms: Boolean
) {
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

    private fun bloomOf(flavours: List<Bytes>, sel: IntArray?): ByteBuffer =
        IntVector.open(al, "xx-hashes", false).use { xxHashes ->
            IntVector.open(al, "murmur3-hashes", false).use { murmur3Hashes ->
                for (flavour in flavours) {
                    flavour.hashTo(xxHashes, Hasher.Xx(), sel)
                    flavour.hashTo(murmur3Hashes, Hasher.Murmur3(), sel)
                }

                val positions = bloomPositions(xxHashes, murmur3Hashes)
                RoaringSerializer.serialize(positions, positions.size)
            }
        }

    private fun writeBytesMetadata(bloom: ByteBuffer?) {
        val typeVec = colsVec.vectorFor(Bytes::class.java.metaColName, STRUCT_TYPE, false)
        val bloomVec = typeVec.vectorFor("bloom", VAR_BINARY_TYPE, true)

        if (bloom != null) bloomVec.writeBytes(bloom)

        typeVec.endStruct()
    }

    private fun writeMetadata(col: VectorReader, rootCol: Boolean, mask: RowMask): RowIndex {
        val withBloom = calculateBlooms || (col.name == "_iid" && rootCol)

        val childRefWrites = mutableListOf<() -> Unit>()
        var numbers: DoubleMinMax? = null
        var dateTimes: DoubleMinMax? = null
        var times: DoubleMinMax? = null
        var durations: DoubleMinMax? = null
        val bytesFlavours = mutableListOf<Bytes>()
        val presence = mutableListOf<Presence>()

        fun DoubleMinMax?.fold(flavour: Numeric) = (this ?: DoubleMinMax()).also { flavour.minMaxInto(it, mask) }

        for (flavour in col.metadataFlavours) {
            when (flavour) {
                is MetadataFlavour.List -> {
                    val childIdx = writeMetadata(flavour.listElements, false, RowMask.all(flavour.listElements.valueCount))
                    childRefWrites += { colsVec.vectorFor("list", I32_TYPE, true).writeInt(childIdx) }
                }

                is MetadataFlavour.Set -> {
                    val childIdx = writeMetadata(flavour.listElements, false, RowMask.all(flavour.listElements.valueCount))
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

                is Bytes -> bytesFlavours += flavour

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

        if (bytesFlavours.isNotEmpty())
            writeBytesMetadata(if (withBloom) bloomOf(bytesFlavours, mask.selection()) else null)

        presence.forEach { colsVec.vectorFor(it.arrowType.toLeg(), BOOL_TYPE, true).writeBoolean(true) }

        colsVec.endStruct()

        return colsVec.valueCount - 1
    }

    fun writeMetadata(col: VectorReader) = writeMetadata(col, true, RowMask.all(col.valueCount))
}

private fun RowMask.selection(): IntArray? = if (cardinality == rowCount) null else toSelection()