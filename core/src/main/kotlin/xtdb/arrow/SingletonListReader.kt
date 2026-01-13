package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import xtdb.util.closeOnCatch

class SingletonListReader(override val name: String, private val elReader: VectorReader) : VectorReader {
    override val nullable = false
    override val arrowType: ArrowType = LIST_TYPE
    override val childFields: List<Field> get() = listOf(elReader.field)

    override val valueCount = 1

    override fun isNull(idx: Int) = false

    override fun getObject(idx: Int) = listOf(elReader.asList)
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = listOf(elReader.toList(keyFn))

    override val listElements get() = elReader
    override fun getListStartIndex(idx: Int) = 0
    override fun getListCount(idx: Int) = elReader.valueCount

    override fun rowCopier(dest: VectorWriter): RowCopier {
        val elCopier = elReader.rowCopier(dest.getListElements(elReader.arrowType, elReader.nullable))

        return RowCopier { idx ->
            check(idx == 0)

            elCopier.copyRange(0, valueCount)
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

    override fun valueReader() = object : ValueReader {
        val startIdx = getListStartIndex(0)
        val valueCount = getListCount(0)
        val elValueReader = elReader.valueReader()

        override fun readObject() = object : ListValueReader {
            override fun size() = valueCount

            override fun nth(idx: Int): ValueReader {
                elValueReader.pos = startIdx + idx
                return elValueReader
            }
        }
    }

    override fun openSlice(al: BufferAllocator) =
        elReader.openSlice(al).closeOnCatch { slicedEl -> SingletonListReader(name, slicedEl) }

    override fun close() = elReader.close()
}