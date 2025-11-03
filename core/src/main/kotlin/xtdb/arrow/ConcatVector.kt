package xtdb.arrow

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.toLeg
import xtdb.trie.ColumnName
import xtdb.types.MergeTypes.Companion.mergeFields
import xtdb.types.Type.Companion.ofType
import xtdb.util.Hasher
import xtdb.util.closeAllOnCatch
import xtdb.util.safeMap
import java.nio.ByteBuffer

class ConcatVector private constructor(
    override val name: ColumnName,
    private val readers: List<VectorReader>,
    private val offsets: IntArray,
) : VectorReader {

    private val fields = readers.map { it.field }
    override val nullable get() = this.field.isNullable

    override val field by lazy(LazyThreadSafetyMode.PUBLICATION) {
        name ofType mergeFields(fields)
    }

    override val fieldType: FieldType get() = this.field.fieldType

    companion object {
        private class Overlay(private val startIdx: Int, private val len: Int) : VectorIndirection {
            override fun getIndex(idx: Int): Int = idx - startIdx
            override fun valueCount(): Int = startIdx + len
        }

        fun from(name: ColumnName, readers: List<VectorReader>): ConcatVector {
            var offset = 0
            val offsets = IntArrayList(readers.size + 1)

            val readers = readers
                .filter { it.valueCount > 0 }
                .map { reader ->
                    offsets.add(offset)
                    val vc = reader.valueCount
                    IndirectVector(reader, Overlay(offset, vc))
                        .also { offset += vc }
                }

            offsets.add(offset)
            return ConcatVector(name, readers, offsets.toArray())
        }
    }

    private fun readerIdx(idx: Int): Int =
        offsets.binarySearch(idx)
            // non-neg -> exact match -> first index of that reader
            // neg -> insertion point -> somewhere in the previous reader
            .let { if (it >= 0) it else -(it + 1) - 1 }

    override val valueCount get() = offsets.last()

    override fun hashCode(idx: Int, hasher: Hasher) = readers[readerIdx(idx)].hashCode(idx, hasher)

    override fun isNull(idx: Int) = readers[readerIdx(idx)].isNull(idx)
    override fun getBoolean(idx: Int) = readers[readerIdx(idx)].getBoolean(idx)
    override fun getByte(idx: Int) = readers[readerIdx(idx)].getByte(idx)
    override fun getShort(idx: Int) = readers[readerIdx(idx)].getShort(idx)
    override fun getInt(idx: Int) = readers[readerIdx(idx)].getInt(idx)
    override fun getLong(idx: Int) = readers[readerIdx(idx)].getLong(idx)
    override fun getFloat(idx: Int) = readers[readerIdx(idx)].getFloat(idx)
    override fun getDouble(idx: Int) = readers[readerIdx(idx)].getDouble(idx)
    override fun getBytes(idx: Int) = readers[readerIdx(idx)].getBytes(idx)
    override fun getPointer(idx: Int, reuse: ArrowBufPointer) = readers[readerIdx(idx)].getPointer(idx, reuse)
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = readers[readerIdx(idx)].getObject(idx, keyFn)

    override fun vectorForOrNull(name: String): VectorReader {
        val childReaders = readers.map { it.vectorForOrNull(name) ?: NullVector(name, true, it.valueCount) }
        return ConcatVector(name, childReaders, offsets)
    }

    override val listElements by lazy {
        from($$"$data$", readers.map { it.listElements })
    }

    override fun getListStartIndex(idx: Int): Int {
        val rIdx = readerIdx(idx)
        val listElementOffset = listElements.offsets[rIdx]
        return listElementOffset + readers[rIdx].getListStartIndex(idx)
    }

    override fun getListCount(idx: Int) = readers[readerIdx(idx)].getListCount(idx)

    override val mapKeys: VectorReader
        get() {
            val keyReaders = readers.map { it.mapKeys }
            return ConcatVector($$"$keys$", keyReaders, offsets)
        }

    override val mapValues: VectorReader
        get() {
            val valueReaders = readers.map { it.mapValues }
            return ConcatVector($$"$values$", valueReaders, offsets)
        }

    override fun getLeg(idx: Int) = readerIdx(idx).let { rIdx ->
        when (val type = fields[rIdx].fieldType.type) {
            is ArrowType.Union -> readers[rIdx].getLeg(idx)
            else -> type.toLeg()
        }
    }

    override fun rowCopier(dest: VectorWriter): RowCopier {
        val rowCopiers = readers.map { it.rowCopier(dest) }
        return object : RowCopier {
            override fun copyRow(srcIdx: Int) = rowCopiers[readerIdx(srcIdx)].copyRow(srcIdx)

            override fun copyRange(startIdx: Int, len: Int) {
                val endIdx = startIdx + len - 1
                val startReaderIdx = readerIdx(startIdx)
                val endReaderIdx = readerIdx(endIdx)

                if (startReaderIdx == endReaderIdx) {
                    // Entire range is in one reader - batch copy
                    rowCopiers[startReaderIdx].copyRange(startIdx, len)
                } else {
                    // Range spans multiple readers - copy per reader
                    var currentIdx = startIdx
                    var remaining = len

                    for (rIdx in startReaderIdx..endReaderIdx) {
                        val readerEnd = offsets[rIdx + 1]
                        val chunkLen = minOf(remaining, readerEnd - currentIdx)
                        rowCopiers[rIdx].copyRange(currentIdx, chunkLen)
                        currentIdx += chunkLen
                        remaining -= chunkLen
                    }
                }
            }
        }
    }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val valueReaders = readers.map { it.valueReader(pos) }

        return object : ValueReader {
            private fun valueReader(): ValueReader = valueReaders[readerIdx(pos.position)]

            override val leg: String? get() = getLeg(pos.position)

            override val isNull: Boolean get() = valueReader().isNull

            override fun readBoolean(): Boolean = valueReader().readBoolean()
            override fun readByte(): Byte = valueReader().readByte()
            override fun readShort(): Short = valueReader().readShort()
            override fun readInt(): Int = valueReader().readInt()
            override fun readLong(): Long = valueReader().readLong()
            override fun readFloat(): Float = valueReader().readFloat()
            override fun readDouble(): Double = valueReader().readDouble()
            override fun readBytes(): ByteBuffer = valueReader().readBytes()
            override fun readObject(): Any? = valueReader().readObject()
        }
    }

    override fun openSlice(al: BufferAllocator): VectorReader =
        readers
            .safeMap { it.openSlice(al) }
            .closeAllOnCatch { ConcatVector(name, it, offsets) }

    override fun toString() = VectorReader.toString(this)

    override fun close() = readers.forEach { it.close() }
}
