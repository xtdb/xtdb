package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import xtdb.util.closeOnCatch
import java.nio.ByteBuffer

class IndirectVector(private val inner: VectorReader, private val sel: VectorIndirection) : VectorReader {
    override val name: String get() = inner.name
    override val valueCount: Int get() = sel.valueCount()
    override val nullable: Boolean get() = inner.nullable
    override val fieldType: FieldType get() = inner.fieldType
    override val field: Field get() = inner.field

    override fun isNull(idx: Int) = inner.isNull(sel[idx])
    override fun getBoolean(idx: Int) = inner.getBoolean(sel[idx])
    override fun getByte(idx: Int) = inner.getByte(sel[idx])
    override fun getShort(idx: Int) = inner.getShort(sel[idx])
    override fun getInt(idx: Int) = inner.getInt(sel[idx])
    override fun getLong(idx: Int) = inner.getLong(sel[idx])
    override fun getFloat(idx: Int) = inner.getFloat(sel[idx])
    override fun getDouble(idx: Int) = inner.getDouble(sel[idx])
    override fun getBytes(idx: Int): ByteBuffer = inner.getBytes(sel[idx])
    override fun getPointer(idx: Int, reuse: ArrowBufPointer) = inner.getPointer(sel[idx], reuse)
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = inner.getObject(sel[idx], keyFn)

    override val keyNames get() = inner.keyNames
    override val legNames get() = inner.legNames
    override fun vectorForOrNull(name: String) = inner.vectorForOrNull(name)?.let { IndirectVector(it, sel) }

    override fun getLeg(idx: Int) = inner.getLeg(sel[idx])

    override val listElements get() = inner.listElements
    override fun getListStartIndex(idx: Int) = inner.getListStartIndex(sel[idx])
    override fun getListCount(idx: Int) = inner.getListCount(sel[idx])

    override val mapKeys: VectorReader get() = inner.mapKeys
    override val mapValues: VectorReader get() = inner.mapValues

    override fun hashCode(idx: Int, hasher: Hasher) = inner.hashCode(sel[idx], hasher)

    override fun openSlice(al: BufferAllocator) = IndirectVector(inner.openSlice(al), sel)

    override fun rowCopier(dest: VectorWriter): RowCopier {
        val innerCopier = inner.rowCopier(dest)
        return RowCopier { srcIdx -> innerCopier.copyRow(sel[srcIdx]) }
    }

    override fun close() = inner.close()
}