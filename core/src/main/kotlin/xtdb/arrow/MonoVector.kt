package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.trie.ColumnName
import xtdb.util.Hasher
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.safeMap
import xtdb.vector.extensions.IntervalMDMType
import xtdb.vector.extensions.KeywordType
import xtdb.vector.extensions.OidType
import xtdb.vector.extensions.RegClassType
import xtdb.vector.extensions.RegProcType
import xtdb.vector.extensions.SetType
import xtdb.vector.extensions.TransitType
import xtdb.vector.extensions.TsTzRangeType
import xtdb.vector.extensions.UriType
import xtdb.vector.extensions.UuidType
import java.time.ZoneId

sealed class MonoVector : Vector(), VectorReader, VectorWriter {

    abstract override var name: String
    abstract override var nullable: Boolean
    abstract override val arrowType: ArrowType

    override val childFields get() = vectors.map { it.field }

    abstract override var valueCount: Int; internal set

    internal abstract fun getObject0(idx: Int, keyFn: IKeyFn<*>): Any
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = if (isNull(idx)) null else getObject0(idx, keyFn)

    open fun ensureCapacity(valueCount: Int): Unit = unsupported("ensureCapacity")

    open fun setNull(idx: Int): Unit = unsupported("setNull")
    open fun setBoolean(idx: Int, v: Boolean): Unit = unsupported("setBoolean")
    open fun setInt(idx: Int, v: Int): Unit = unsupported("setInt")
    open fun setLong(idx: Int, v: Long): Unit = unsupported("setLong")
    open fun setFloat(idx: Int, v: Float): Unit = unsupported("setFloat")
    open fun setDouble(idx: Int, v: Double): Unit = unsupported("setDouble")

    override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    protected abstract fun writeObject0(value: Any)

    override fun writeObject(obj: Any?) =
        when (obj) {
            null -> writeNull()
            is ValueReader -> writeValue(obj)
            else -> writeObject0(obj)
        }

    abstract fun hashCode0(idx: Int, hasher: Hasher): Int
    final override fun hashCode(idx: Int, hasher: Hasher) =
        if (isNull(idx)) ArrowBufPointer.NULL_HASH_CODE else hashCode0(idx, hasher)

    abstract override fun openSlice(al: BufferAllocator): MonoVector
    override fun openDirectSlice(al: BufferAllocator) = openSlice(al)

    override fun maybePromote(al: BufferAllocator, targetType: ArrowType, targetNullable: Boolean): Vector =
        // if it's a NullVector coming in, don't promote - we can just set ourselves to nullable. #4675
        when {
            targetType != arrowType && targetType != NULL_TYPE ->
                DenseUnionVector(al, name, listOf(this), valueCount)
                    .apply {
                        repeat(this.valueCount) { idx ->
                            typeBuffer.writeByte(0)
                            offsetBuffer.writeInt(idx)
                        }

                        maybePromote(al, targetType, targetNullable)
                    }
                    .also {
                        this@MonoVector.name = arrowType.toLeg()
                    }

            else -> {
                nullable = nullable || targetNullable
                this
            }
        }
}