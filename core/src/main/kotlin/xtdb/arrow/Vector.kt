package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.ipc.message.ArrowFieldNode

sealed class Vector(val field: Field) : AutoCloseable {
    val name get() = field.name
    abstract val valueCount: Int

    private fun unsupported(op: String): Nothing =
        throw UnsupportedOperationException("$op unsupported on ${this::class.simpleName}")

    abstract fun isNull(idx: Int): Boolean
    abstract fun writeNull()

    open fun getInt(idx: Int): Int = unsupported("getInt")
    open fun setInt(idx: Int, value: Int): Unit = unsupported("setInt")
    open fun writeInt(value: Int): Unit = unsupported("writeInt")

    protected open fun getObject0(idx: Int): Any = unsupported("getObject")

    fun getObject(idx: Int) = if (isNull(idx)) null else getObject0(idx)

    abstract fun writeObject0(value: Any)

    fun writeObject(value: Any?) {
        if (value == null) writeNull() else writeObject0(value)
    }

    internal abstract fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)
    internal abstract fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)

    abstract fun reset()
}
