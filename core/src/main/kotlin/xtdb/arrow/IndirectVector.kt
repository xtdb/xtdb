package xtdb.arrow

import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.query.IKeyFn

class IndirectVector(private val inner: VectorReader, private val sel: VectorIndirection): VectorReader {
    override val name: String get() = inner.name
    override val valueCount: Int get() = sel.valueCount()
    override val nullable: Boolean get() = inner.nullable
    override val field: Field get() = inner.field

    override fun isNull(idx: Int) = inner.isNull(sel[idx])
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = inner.getObject(sel[idx], keyFn)

    override fun hashCode(idx: Int, hasher: ArrowBufHasher) = inner.hashCode(sel[idx], hasher)

    override fun rowCopier(dest: VectorWriter): RowCopier {
        val innerCopier = inner.rowCopier(dest)
        return RowCopier { srcIdx -> innerCopier.copyRow(sel[srcIdx]) }
    }

    override fun close() = inner.close()

}