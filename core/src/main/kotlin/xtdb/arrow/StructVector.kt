package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.vector.BitVectorHelper.getValidityBufferSize
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.NonNullableStructVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.error.Incorrect
import xtdb.toFieldType
import xtdb.util.Hasher
import xtdb.util.normalForm
import java.util.*

internal val STRUCT = ArrowType.Struct.INSTANCE

class StructVector
@JvmOverloads constructor(
    private val allocator: BufferAllocator,
    override var name: String, override var nullable: Boolean,
    private val childWriters: SequencedMap<String, Vector> = LinkedHashMap(),
    override var valueCount: Int = 0,
) : Vector(), MetadataFlavour.Struct {

    override val type: ArrowType = STRUCT

    override val vectors: Iterable<Vector> get() = childWriters.sequencedValues()

    private val validityBuffer = ExtensibleBuffer(allocator)

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    override fun writeUndefined() {
        validityBuffer.writeBit(valueCount++, 0)
        childWriters.sequencedValues().forEach { it.writeUndefined() }
    }

    override val keyNames get() = childWriters.keys

    override fun vectorForOrNull(name: String) = childWriters[name]

    override fun vectorFor(name: String) = childWriters[name] ?: error("missing child vector: $name")

    override fun vectorFor(name: String, fieldType: FieldType) =
        childWriters.compute(name) { _, existingChild ->
            when {
                existingChild == null ->
                    fromField(allocator, Field(name, fieldType, emptyList())).also { newVec ->
                        repeat(valueCount) { if (isNull(it)) newVec.writeUndefined() else newVec.writeNull() }
                    }

                existingChild.fieldType.type != fieldType.type || (!existingChild.nullable && fieldType.isNullable) ->
                    existingChild.maybePromote(allocator, fieldType)

                else -> existingChild
            }
        }!!

    override fun endStruct() {
        validityBuffer.writeBit(valueCount++, 1)

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

    override fun valueReader(pos: VectorPosition): ValueReader {
        val readers = childWriters.mapValues { it.value.valueReader(pos) }

        return object : ValueReader {
            override val isNull get() = this@StructVector.isNull(pos.position)

            override fun readObject() = if (isNull) null else readers
        }
    }

    override fun hashCode0(idx: Int, hasher: Hasher) =
        childWriters.values.fold(0) { hash, child ->
            ByteFunctionHelpers.combineHash(hash, child.hashCode(idx, hasher))
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        if (src.fieldType.type != type) throw InvalidCopySourceException(src.fieldType, fieldType)
        nullable = nullable || src.nullable

        check(src is StructVector)
        val childCopiers = src.childWriters.map { (childName, child) ->
            child.rowCopier(vectorFor(childName, child.fieldType))
        }

        val srcKeys = src.childWriters.keys
        for (child in childWriters.values) {
            if (child.name !in srcKeys) child.nullable = true
        }

        return RowCopier { srcIdx ->
            childCopiers.forEach { it.copyRow(srcIdx) }
            valueCount.also { endStruct() }
        }
    }

    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)

        childWriters.sequencedValues().forEach { it.unloadPage(nodes, buffers) }
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")

        validityBuffer.loadBuffer(buffers.removeFirstOrNull() ?: error("missing validity buffer"))
        childWriters.sequencedValues().forEach { it.loadPage(nodes, buffers) }

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is NonNullableStructVector)
        val valCount = vec.valueCount
        validityBuffer.loadBuffer(vec.validityBuffer, getValidityBufferSize(valCount).toLong())
        childWriters.sequencedValues().forEach { it.loadFromArrow(vec.getChild(it.name)) }

        valueCount = valCount
    }

    override fun openSlice(al: BufferAllocator) =
        StructVector(
            al, name, nullable,
            childWriters.entries.associateTo(LinkedHashMap()) { it.key to it.value.openSlice(al) },
            valueCount
        )

    override fun clear() {
        validityBuffer.clear()
        valueCount = 0
        childWriters.sequencedValues().forEach(Vector::clear)
    }

    override fun close() {
        validityBuffer.close()
        childWriters.sequencedValues().forEach(Vector::close)
    }
}
