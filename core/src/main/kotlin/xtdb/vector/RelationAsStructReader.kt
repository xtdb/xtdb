package xtdb.vector

import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.RowCopier
import xtdb.arrow.ValueReader
import xtdb.arrow.VectorPosition
import xtdb.util.Hasher
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE

class RelationAsStructReader(
    private val name: String,
    private val rel: RelationReader
) : IVectorReader {
    override fun valueCount() = rel.rowCount()
    override fun getName() = name

    override fun getField() =
        Field(name, FieldType.notNullable(STRUCT_TYPE), rel.map(IVectorReader::getField))

    override fun structKeys() = rel.map { it.name }
    override fun structKeyReader(colName: String): IVectorReader = rel.readerForName(colName)

    override fun getObject(idx: Int): Any = rel.getRow(idx)

    override fun getObject(idx: Int, keyFn: IKeyFn<*>): Any = rel.getRow(idx, keyFn)

    override fun copyTo(vector: ValueVector) = TODO("Not yet implemented")

    override fun rowCopier(writer: IVectorWriter): RowCopier {
        val copiers = rel.map { it.rowCopier(writer) }
        return RowCopier { idx -> copiers.forEach { it.copyRow(idx) }; idx }
    }

    override fun valueReader(pos: VectorPosition?): ValueReader {
        val rdrs = rel.associate { it.name to it.valueReader(pos) }

        return object : ValueReader {
            override val isNull get() = false
            override fun readObject() = rdrs
        }
    }

    override fun hashCode(idx: Int, hasher: Hasher): Int =
        rel.fold(0) { hash, col -> ByteFunctionHelpers.combineHash(hash, col.hashCode(idx, hasher)) }

    override fun close() = rel.close()
}