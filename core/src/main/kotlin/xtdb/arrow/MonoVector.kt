package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.api.query.IKeyFn
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Mono
import xtdb.util.Hasher

sealed class MonoVector : Vector(), VectorReader, VectorWriter {

    abstract override var name: String
    abstract override var nullable: Boolean

    abstract override var valueCount: Int; internal set

    internal abstract val monoType: Mono
    override val type get() = maybe(monoType, nullable)

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