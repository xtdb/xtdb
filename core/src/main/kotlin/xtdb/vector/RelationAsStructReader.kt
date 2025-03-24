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
    override val name: String,
    private val rel: RelationReader
) : IVectorReader {
    override val valueCount get() = rel.rowCount

    override val field get() =
        Field(name, FieldType.notNullable(STRUCT_TYPE), rel.vectors.map(IVectorReader::field))

    override fun isNull(idx: Int) = false

    override val keyNames get() = rel.vectors.map { it.name }.toSet()
    override fun structKeyReader(colName: String): IVectorReader? = rel.vectorForOrNull(colName)

    override fun getObject(idx: Int): Any = rel[idx]

    override fun getObject(idx: Int, keyFn: IKeyFn<*>): Any = rel[idx, keyFn]

    override fun copyTo(vector: ValueVector) = TODO("Not yet implemented")

    override fun rowCopier(writer: IVectorWriter): RowCopier {
        val copiers = rel.vectors.map { it.rowCopier(writer) }
        return RowCopier { idx -> copiers.forEach { it.copyRow(idx) }; idx }
    }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val rdrs = rel.vectors.associate { it.name to it.valueReader(pos) }

        return object : ValueReader {
            override val isNull get() = false
            override fun readObject() = rdrs
        }
    }

    override fun hashCode(idx: Int, hasher: Hasher): Int =
        rel.vectors.fold(0) { hash, col -> ByteFunctionHelpers.combineHash(hash, col.hashCode(idx, hasher)) }

    override fun close() = rel.close()
}