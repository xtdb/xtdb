package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import xtdb.util.closeOnCatch

class SingletonListReader(override val name: String, private val elReader: VectorReader) : VectorReader {
    override val nullable = false
    override val fieldType: FieldType = FieldType.notNullable(ArrowType.List.INSTANCE)
    override val valueCount get() = 1
    override val field get() = Field(name, FieldType.notNullable(ArrowType.List.INSTANCE), listOf(elReader.field))

    override fun isNull(idx: Int) = false

    override fun getObject(idx: Int) = listOf(elReader.toList())
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = listOf(elReader.toList(keyFn))

    override val listElements get() = elReader
    override fun getListStartIndex(idx: Int) = 0
    override fun getListCount(idx: Int) = elReader.valueCount

    override fun rowCopier(dest: VectorWriter): RowCopier {
        val elCopier = elReader.rowCopier(dest.getListElements(elReader.field.fieldType))

        return RowCopier { idx ->
            check(idx == 0)

            for (i in 0 until valueCount) {
                elCopier.copyRow(i)
            }
            dest.endList()
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

    override fun openSlice(al: BufferAllocator) =
        elReader.openSlice(al).closeOnCatch { slicedEl -> SingletonListReader(name, slicedEl) }

    override fun close() = elReader.close()
}