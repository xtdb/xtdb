package xtdb.vector

import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.ListValueReader
import xtdb.arrow.RowCopier
import xtdb.arrow.ValueReader
import xtdb.arrow.VectorPosition
import xtdb.util.Hasher
import org.apache.arrow.vector.types.pojo.ArrowType.List.INSTANCE as LIST_TYPE

class SingletonListReader(override val name: String, private val elReader: IVectorReader) : IVectorReader {
    override val valueCount get() = 1
    override val field get() = Field(name, FieldType.notNullable(LIST_TYPE), listOf(elReader.field))

    override fun isNull(idx: Int) = false

    override fun getObject(idx: Int) = listOf(elReader.toList())
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = listOf(elReader.toList(keyFn))

    override val listElements get() = elReader
    override fun getListStartIndex(idx: Int) = 0
    override fun getListCount(idx: Int) = elReader.valueCount

    override fun copyTo(vector: ValueVector) = TODO("Not yet implemented")

    override fun rowCopier(writer: IVectorWriter): RowCopier {
        val elCopier = elReader.rowCopier(writer.getListElements(elReader.field.fieldType))

        return RowCopier { idx ->
            check(idx == 0)

            for (i in 0 until valueCount) {
                elCopier.copyRow(i)
            }
            writer.endList()
            idx
        }
    }

    override fun hashCode(idx: Int, hasher: Hasher): Int {
        var hash = 0
        for (i in 0 until valueCount) {
            hash = ByteFunctionHelpers.combineHash(hash, elReader.hashCode(i, hasher))
        }
        return hash
    }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val elPos = VectorPosition.build()
        val elValueReader = elReader.valueReader(elPos)

        return object : ValueReader {
            val startIdx = getListStartIndex(pos.position)
            val valueCount = getListCount(pos.position)

            override fun readObject() = object : ListValueReader {
                override fun size() = valueCount

                override fun nth(idx: Int): ValueReader {
                    elPos.position = startIdx + idx
                    return elValueReader
                }
            }
        }
    }

    override fun close() = elReader.close()
}
