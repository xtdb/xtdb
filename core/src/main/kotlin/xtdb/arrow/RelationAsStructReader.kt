package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.query.IKeyFn
import xtdb.types.Fields
import xtdb.util.Hasher
import xtdb.util.closeOnCatch

class RelationAsStructReader(
    override val name: String,
    private val rel: RelationReader
) : VectorReader {
    override val nullable = false
    override val fieldType = Fields.Struct().fieldType
    override val field: Field get() = Fields.Struct(rel.vectors.map { it.field }).toArrowField(name)

    override val valueCount get() = rel.rowCount

    override fun isNull(idx: Int) = false

    override val keyNames get() = rel.vectors.map { it.name }.toSet()

    override fun getObject(idx: Int): Any = rel[idx]

    override fun getObject(idx: Int, keyFn: IKeyFn<*>): Any = rel[idx, keyFn]

    override fun rowCopier(dest: VectorWriter): RowCopier {
        val copiers = rel.vectors.map { it.rowCopier(dest.vectorFor(it.name, it.fieldType)) }
        return RowCopier { idx ->
            copiers.forEach { it.copyRow(idx) }
            dest.endStruct()
            idx
        }
    }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val rdrs = rel.vectors.associate { it.name to it.valueReader(pos) }

        return object : ValueReader {
            override val isNull get() = false
            override fun readObject() = rdrs
        }
    }

    override fun openSlice(al: BufferAllocator) =
        rel.openSlice(al)
            .closeOnCatch { slicedRel -> RelationAsStructReader(name, slicedRel) }

    override fun hashCode(idx: Int, hasher: Hasher): Int =
        rel.vectors.fold(0) { hash, col -> ByteFunctionHelpers.combineHash(hash, col.hashCode(idx, hasher)) }

    override fun close() = rel.close()
}