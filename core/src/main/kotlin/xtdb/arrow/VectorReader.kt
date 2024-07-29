package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.query.IKeyFn
import xtdb.vector.*
import java.nio.ByteBuffer

interface VectorReader : AutoCloseable {
    val name: String
    val valueCount: Int

    val nullable: Boolean
    val arrowField: Field

    fun isNull(idx: Int): Boolean
    fun getBoolean(idx: Int): Boolean = unsupported("getBoolean")
    fun getByte(idx: Int): Byte = unsupported("getByte")
    fun getShort(idx: Int): Short = unsupported("getShort")
    fun getInt(idx: Int): Int = unsupported("getInt")
    fun getLong(idx: Int): Long = unsupported("getLong")
    fun getFloat(idx: Int): Float = unsupported("getFloat")
    fun getDouble(idx: Int): Double = unsupported("getDouble")
    fun getBytes(idx: Int): ByteArray = unsupported("getBytes")
    fun getPointer(idx: Int, reuse: ArrowBufPointer? = null): ArrowBufPointer = unsupported("getPointer")
    fun getObject(idx: Int): Any?

    fun elementReader(): VectorReader = unsupported("elementReader")
    fun getListStartIndex(idx: Int): Int = unsupported("getListStartIndex")
    fun getListCount(idx: Int): Int = unsupported("getListCount")

    fun mapKeyReader(): VectorReader = unsupported("mapKeyReader")
    fun mapValueReader(): VectorReader = unsupported("mapValueReader")

    fun keyReader(name: String): VectorReader? = unsupported("keyReader")

    fun legReader(name: String): VectorReader? = unsupported("legWriter")
    fun getLeg(idx: Int): String = unsupported("getLeg")

    fun toList(): List<Any?>

    companion object {
        internal class Adapter(private val vector: VectorReader) : IVectorReader {

            override fun hashCode(idx: Int, hasher: ArrowBufHasher?) = TODO("Not yet implemented")

            override fun valueCount() = vector.valueCount

            override fun getName() = vector.name

            override fun withName(colName: String?) = TODO()

            override fun getField() = vector.arrowField

            override fun isNull(idx: Int) = vector.isNull(idx)
            override fun getBoolean(idx: Int) = vector.getBoolean(idx)
            override fun getByte(idx: Int) = vector.getByte(idx)
            override fun getShort(idx: Int) = vector.getShort(idx)
            override fun getInt(idx: Int) = vector.getInt(idx)
            override fun getLong(idx: Int) = vector.getLong(idx)
            override fun getFloat(idx: Int) = vector.getFloat(idx)
            override fun getDouble(idx: Int) = vector.getDouble(idx)
            override fun getBytes(idx: Int): ByteBuffer = ByteBuffer.wrap(vector.getBytes(idx))

            override fun getPointer(idx: Int) = vector.getPointer(idx)
            override fun getPointer(idx: Int, reuse: ArrowBufPointer) = vector.getPointer(idx, reuse)

            override fun getObject(idx: Int) = vector.getObject(idx)
            override fun getObject(idx: Int, keyFn: IKeyFn<*>?) = TODO()

            override fun structKeyReader(colName: String) = vector.keyReader(colName)?.let { Adapter(it) }

            override fun structKeys() = TODO()

            override fun listElementReader() = Adapter(vector.elementReader())
            override fun getListStartIndex(idx: Int) = vector.getListStartIndex(idx)
            override fun getListCount(idx: Int) = vector.getListCount(idx)

            override fun mapKeyReader() = Adapter(vector.mapKeyReader())
            override fun mapValueReader() = Adapter(vector.mapValueReader())

            override fun getLeg(idx: Int): Keyword = Keyword.intern(vector.getLeg(idx))

            override fun legReader(legKey: Keyword) = vector.legReader(legKey.sym.toString())?.let { Adapter(it) }

            override fun legs() = error("legs")

            override fun copyTo(vector: ValueVector?) = error("copyTo")
            override fun transferTo(vector: ValueVector?) = error("transferTo")

            override fun rowCopier(writer: IVectorWriter?) = error("rowCopier")

            private inner class ValueReader(private val pos: IVectorPosition) : IValueReader {

                override val leg get() = getLeg(pos.position)

                override val isNull get() = isNull(pos.position)

                override fun readBoolean() = getBoolean(pos.position)
                override fun readByte() = getByte(pos.position)
                override fun readShort() = getShort(pos.position)
                override fun readInt() = getInt(pos.position)
                override fun readLong() = getLong(pos.position)
                override fun readFloat() = getFloat(pos.position)
                override fun readDouble() = getDouble(pos.position)
                override fun readBytes() = getBytes(pos.position)
                override fun readObject() = getObject(pos.position)
            }

            override fun valueReader(pos: IVectorPosition): IValueReader = ValueReader(pos)

            override fun close() = vector.close()

        }
    }
}