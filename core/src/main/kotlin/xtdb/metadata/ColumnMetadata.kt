package xtdb.metadata

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.vector.types.pojo.FieldType.notNullable
import org.apache.arrow.vector.types.pojo.FieldType.nullable
import xtdb.arrow.*
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.arrow.metadata.MetadataFlavour.*
import xtdb.arrow.metadata.MetadataFlavour.Companion.metaColName
import xtdb.bloom.BloomBuilder
import xtdb.bloom.toByteBuffer
import xtdb.toLeg
import xtdb.trie.RowIndex
import kotlin.reflect.KClass

class ColumnMetadata(private val colsVec: VectorWriter, private val calculateBlooms: Boolean) {
    private val colNameVec = colsVec["col-name"]
    private val rootColVec = colsVec["root-col?"]
    private val countVec = colsVec["count"]

    private fun writeNumericMetadata(flavours: List<Numeric>, type: KClass<out Numeric>) {
        if (flavours.isNotEmpty()) {
            val typeVec = colsVec.vectorFor(type.java.metaColName, nullable(STRUCT))
            val minVec = typeVec.vectorFor("min", notNullable(F64))
            val maxVec = typeVec.vectorFor("max", notNullable(F64))

            var minValue = Double.POSITIVE_INFINITY
            var maxValue = Double.NEGATIVE_INFINITY

            for (flavour in flavours) {
                repeat(flavour.valueCount) {
                    if (!flavour.isNull(it)) {
                        val value = flavour.getMetaDouble(it)
                        minValue = minValue.coerceAtMost(value)
                        maxValue = maxValue.coerceAtLeast(value)
                    }
                }
            }

            minVec.writeDouble(minValue)
            maxVec.writeDouble(maxValue)
            typeVec.endStruct()
        }
    }

    private fun writeBytesMetadata(flavours: List<Bytes>, calculateBlooms: Boolean) {
        if (flavours.isNotEmpty()) {
            val typeVec = colsVec.vectorFor(Bytes::class.java.metaColName, nullable(STRUCT))
            val bloomVec = typeVec.vectorFor("bloom", nullable(VAR_BINARY))

            if (calculateBlooms) {
                val bloomBuilder = BloomBuilder()
                flavours.forEach { bloomBuilder.add(it) }
                bloomVec.writeBytes(bloomBuilder.build().toByteBuffer())
            }

            typeVec.endStruct()
        }
    }

    private fun writePresenceMetadata(flavours: List<Presence>) {
        flavours.forEach {
            colsVec.vectorFor(it.type.toLeg(), nullable(BOOL_TYPE)).writeBoolean(true)
        }
    }

    private fun writeMetadata(col: VectorReader, rootCol: Boolean): RowIndex {
        val flavours = col.metadataFlavours

        val childIdxs = IntArrayList()

        for (flavour in flavours) {
            when (flavour) {
                is MetadataFlavour.List -> childIdxs.add(writeMetadata(flavour.listElements, false))
                is MetadataFlavour.Set -> childIdxs.add(writeMetadata(flavour.listElements, false))
                is Struct -> flavour.vectors.forEach { childIdxs.add(writeMetadata(it, false)) }
                else -> Unit
            }
        }

        var childIdx = 0

        val bytes = mutableListOf<Bytes>()
        val dateTimes = mutableListOf<Numeric>()
        val durations = mutableListOf<Numeric>()
        val numbers = mutableListOf<Numeric>()
        val times = mutableListOf<Numeric>()
        val presence = mutableListOf<Presence>()

        for (flavour in flavours) {
            when (flavour) {
                is Bytes -> bytes.add(flavour)

                is MetadataFlavour.List ->
                    colsVec.vectorFor("list", nullable(I32))
                        .writeInt(childIdxs[childIdx++])

                is MetadataFlavour.Set ->
                    colsVec.vectorFor("set", nullable(I32))
                        .writeInt(childIdxs[childIdx++])

                is DateTime -> dateTimes.add(flavour)
                is Duration -> durations.add(flavour)
                is MetadataFlavour.Number -> numbers.add(flavour)
                is TimeOfDay -> times.add(flavour)

                is Struct -> {
                    val keysVec = colsVec.vectorFor("struct", nullable(LIST))
                    val keyVec = keysVec.getListElements(notNullable(I32))

                    repeat(flavour.vectors.count()) { keyVec.writeInt(childIdxs[childIdx++]) }

                    keysVec.endList()
                }

                is Presence -> presence.add(flavour)
            }
        }

        assert(childIdx == childIdxs.size()) { "haven't used up all the nested vectors" }

        colNameVec.writeObject(col.name)
        rootColVec.writeBoolean(rootCol)
        countVec.writeLong((0 until col.valueCount).count { !col.isNull(it) }.toLong())

        writeNumericMetadata(numbers, MetadataFlavour.Number::class)
        writeNumericMetadata(dateTimes, DateTime::class)
        writeNumericMetadata(times, TimeOfDay::class)
        writeNumericMetadata(durations, Duration::class)

        writeBytesMetadata(bytes, calculateBlooms = calculateBlooms || (col.name == "_iid" && rootCol))

        writePresenceMetadata(presence)

        colsVec.endStruct()

        return colsVec.valueCount - 1
    }

    fun writeMetadata(col: VectorReader) = writeMetadata(col, true)
}