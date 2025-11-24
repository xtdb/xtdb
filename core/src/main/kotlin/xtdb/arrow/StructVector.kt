package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.NonNullableStructVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.error.Incorrect
import xtdb.util.Hasher
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.normalForm
import java.util.*

internal val STRUCT = ArrowType.Struct.INSTANCE

class StructVector private constructor(
    private val allocator: BufferAllocator,
    override var name: String,
    private var validityBuffer: BitBuffer?,
    private val childWriters: SequencedMap<String, Vector> = LinkedHashMap(),
    override var valueCount: Int = 0,
) : Vector(), MetadataFlavour.Struct {

    override var nullable: Boolean
        get() = validityBuffer != null
        set(value) {
            if (value && validityBuffer == null)
                BitBuffer(allocator).also { validityBuffer = it }.writeOnes(valueCount)
        }

    @JvmOverloads
    constructor(
        allocator: BufferAllocator,
        name: String, nullable: Boolean,
        childWriters: SequencedMap<String, Vector> = LinkedHashMap(),
        valueCount: Int = 0,
    ) : this(
        allocator, name,
        if (nullable) BitBuffer(allocator) else null, childWriters, valueCount
    )

    override val arrowType: ArrowType = STRUCT

    override val vectors: Iterable<Vector> get() = childWriters.sequencedValues()

    override fun isNull(idx: Int) = nullable && !validityBuffer!!.getBoolean(idx)

    override fun writeUndefined() {
        validityBuffer?.writeBit(valueCount, 0)
        valueCount++
        childWriters.sequencedValues().forEach { it.writeUndefined() }
    }

    override val keyNames get() = childWriters.keys

    override fun vectorForOrNull(name: String) = childWriters[name]

    override fun vectorFor(name: String) = childWriters[name] ?: error("missing child vector: $name")

    override fun vectorFor(name: String, fieldType: FieldType) =
        childWriters.compute(name) { _, existingChild ->
            when {
                existingChild == null ->
                    Field(name, fieldType, emptyList()).openVector(allocator).also { newVec ->
                        repeat(valueCount) { if (isNull(it)) newVec.writeUndefined() else newVec.writeNull() }
                    }

                existingChild.fieldType.type != fieldType.type || (!existingChild.nullable && fieldType.isNullable) ->
                    existingChild.maybePromote(allocator, fieldType)

                else -> existingChild
            }
        }!!

    override fun endStruct() {
        validityBuffer?.writeBit(valueCount, 1)
        valueCount++

        val valueCount = valueCount

        childWriters.sequencedValues().forEach { child ->
            repeat(valueCount - child.valueCount) { child.writeNull() }
        }
    }

    override val mapKeys get() = childWriters.sequencedValues().firstOrNull() ?: TODO("auto-creation")
    override val mapValues get() = childWriters.sequencedValues().lastOrNull() ?: TODO("auto-creation")

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Any =
        childWriters.sequencedEntrySet()
            .associateBy({ keyFn.denormalize(it.key) }, { it.value.getObject(idx, keyFn) })
            .filterValues { it != null }

    override val metadataFlavours get() = listOf(this)

    private fun keyString(key: Any?): String = when (key) {
        is String -> key
        is Keyword -> normalForm(key.sym).toString()
        else -> error("invalid key type: $key")
    }

    override fun writeObject0(value: Any) =
        if (value !is Map<*, *>) throw InvalidWriteObjectException(fieldType, value)
        else {
            value.forEach {
                val key = keyString(it.key)
                val obj = it.value
                val childWriter = childWriters[key] ?: vectorFor(key, obj.toFieldType())

                if (childWriter.valueCount != this.valueCount)
                    throw Incorrect(
                        errorCode = "xtdb/key-already-set",
                        data = mapOf("ks" to value.keys, "k" to key)
                    )

                try {
                    childWriter.writeObject(obj)
                } catch (e: InvalidWriteObjectException) {
                    val newWriter = childWriter.maybePromote(allocator, e.obj.toFieldType())
                    childWriters[key] = newWriter
                    newWriter.writeObject(obj)
                }
            }
            endStruct()
        }

    override fun writeValue0(v: ValueReader) = writeObject(v.readObject())

    override fun valueReader() = object : ValueReader {
        val readers = childWriters.mapValues { it.value.valueReader() }

        override var pos = 0
            set(value) {
                field = value
                readers.forEach { it.value.pos = value }
            }

        override val isNull get() = this@StructVector.isNull(pos)

        override fun readObject() = if (isNull) null else readers
    }

    override fun hashCode0(idx: Int, hasher: Hasher) =
        childWriters.values.fold(0) { hash, child ->
            ByteFunctionHelpers.combineHash(hash, child.hashCode(idx, hasher))
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        nullable = nullable || src.nullable

        check(src is StructVector)
        val colNames = (src.childWriters.keys + childWriters.keys)

        val childCopiers = colNames.map { colName ->
            val srcVec = src.vectorForOrNull(colName) ?: NullVector(colName, true, src.valueCount)
            srcVec.rowCopier(vectorFor(colName, srcVec.fieldType))
        }

        return RowCopier { srcIdx ->
            if (src.isNull(srcIdx)) {
                writeNull()
            } else {
                childCopiers.forEach { it.copyRow(srcIdx) }
                endStruct()
            }
        }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), if (nullable) -1 else 0))
        if (nullable) validityBuffer?.unloadBuffer(buffers) else buffers.add(allocator.empty)

        childWriters.sequencedValues().forEach { it.unloadPage(nodes, buffers) }
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")
        valueCount = node.length

        val validityBuf = buffers.removeFirstOrNull() ?: error("missing validity buffer")
        validityBuffer?.loadBuffer(validityBuf, valueCount)
        childWriters.sequencedValues().forEach { it.loadPage(nodes, buffers) }
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is NonNullableStructVector)
        val valCount = vec.valueCount
        validityBuffer?.loadBuffer(vec.validityBuffer, valCount)
        childWriters.sequencedValues().forEach { it.loadFromArrow(vec.getChild(it.name)) }

        valueCount = valCount
    }

    override fun openSlice(al: BufferAllocator) =
        validityBuffer?.openSlice(al).closeOnCatch { validityBuffer ->
            StructVector(
                al, name, validityBuffer,
                LinkedHashMap<String, Vector>().closeAllOnCatch { cws ->
                    childWriters.entries.associateTo(cws) { it.key to it.value.openSlice(al) }
                },
                valueCount
            )
        }


    override fun clear() {
        validityBuffer?.clear()
        valueCount = 0
        childWriters.sequencedValues().forEach(Vector::clear)
    }

    override fun close() {
        validityBuffer?.close()
        childWriters.sequencedValues().forEach(Vector::close)
    }
}
