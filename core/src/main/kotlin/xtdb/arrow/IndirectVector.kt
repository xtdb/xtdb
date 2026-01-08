package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.agg.VectorSummer
import xtdb.util.Hasher
import java.nio.ByteBuffer

class IndirectVector(private val inner: VectorReader, private val sel: VectorIndirection) : VectorReader, LongLongVectorReader {
    override val name: String get() = inner.name
    override val valueCount: Int get() = sel.valueCount()
    override val nullable: Boolean get() = inner.nullable
    override val arrowType: ArrowType get() = inner.arrowType
    val fieldType: FieldType get() = inner.type.fieldType
    override val field: Field get() = inner.field
    override val childFields: List<Field> get() = inner.childFields

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

    override fun getLongLongHigh(idx: Int) = (inner as LongLongVectorReader).getLongLongHigh(sel[idx])
    override fun getLongLongLow(idx: Int) = (inner as LongLongVectorReader).getLongLongLow(sel[idx])

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

    override fun select(idxs: IntArray): VectorReader =
        inner.select(sel.select(idxs))

    override fun select(startIdx: Int, len: Int): VectorReader =
        IndirectVector(inner, selection(sel.select(startIdx, len)))

    override fun rowCopier(dest: VectorWriter): RowCopier {
        val innerCopier = inner.rowCopier(dest)
        return object : RowCopier {
            override fun copyRow(srcIdx: Int) = innerCopier.copyRow(sel[srcIdx])

            override fun copyRows(sel: IntArray) {
                val translatedSel = IntArray(sel.size) { this@IndirectVector.sel[sel[it]] }
                innerCopier.copyRows(translatedSel)
            }

            override fun copyRange(startIdx: Int, len: Int) {
                val translatedSel = IntArray(len) { this@IndirectVector.sel[startIdx + it] }
                innerCopier.copyRows(translatedSel)
            }
        }
    }

    override fun valueReader(): ValueReader {
        val inner = inner.valueReader()
        return object : ValueReader by inner {
            val sel = this@IndirectVector.sel
            val valueCount = sel.valueCount()

            override var pos: Int = 0
                set(value) {
                    field = value
                    if (value in 0..<valueCount)
                        inner.pos = sel[value]
                }
        }
    }

    override fun sumInto(outVec: Vector): VectorSummer {
        val inner = inner.sumInto(outVec)
        return VectorSummer { idx, groupIdx -> inner.sumRow(sel[idx], groupIdx) }
    }

    override fun squareInto(outVec: Vector): VectorReader = IndirectVector(inner.squareInto(outVec), sel)

    override fun sqrtInto(outVec: Vector): VectorReader = IndirectVector(inner.sqrtInto(outVec), sel)

    override fun close() = inner.close()
}