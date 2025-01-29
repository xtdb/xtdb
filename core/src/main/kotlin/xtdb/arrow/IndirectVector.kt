package xtdb.arrow

import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import java.nio.ByteBuffer

class IndirectVector(private val inner: VectorReader, private val sel: VectorIndirection) : VectorReader {
    override val name: String get() = inner.name
    override val valueCount: Int get() = sel.valueCount()
    override val nullable: Boolean get() = inner.nullable
    override val fieldType: FieldType get() = this.field.fieldType
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

    override val legs get() = inner.legs
    override fun getLeg(idx: Int) = inner.getLeg(sel[idx])
    override fun legReader(name: String) = inner.legReader(name)?.let { IndirectVector(it, sel) }

    override val keys get() = inner.keys
    override fun keyReader(name: String) = inner.keyReader(name)?.let { IndirectVector(it, sel) }

    override fun elementReader() = inner.elementReader()
    override fun getListStartIndex(idx: Int) = inner.getListStartIndex(sel[idx])
    override fun getListCount(idx: Int) = inner.getListCount(sel[idx])

    override fun hashCode(idx: Int, hasher: Hasher) = inner.hashCode(sel[idx], hasher)

    override fun rowCopier(dest: VectorWriter): RowCopier {
        val innerCopier = inner.rowCopier(dest)
        return RowCopier { srcIdx -> innerCopier.copyRow(sel[srcIdx]) }
    }

    override fun close() = inner.close()

}