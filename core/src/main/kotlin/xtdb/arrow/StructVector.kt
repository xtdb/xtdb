package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.NonNullableStructVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.asKeyword
import xtdb.toFieldType
import xtdb.util.normalForm
import java.util.*

internal val STRUCT_TYPE = ArrowType.Struct.INSTANCE

class StructVector(
    private val allocator: BufferAllocator,
    override var name: String,
    override var fieldType: FieldType,
    private val childWriters: SequencedMap<String, Vector>,
) : Vector() {

    constructor(
        allocator: BufferAllocator,
        name: String,
        nullable: Boolean,
        childrenByKey: SequencedMap<String, Vector> = LinkedHashMap()
    )
            : this(allocator, name, FieldType(nullable, STRUCT_TYPE, null), childrenByKey)

    override val children: Iterable<Vector> get() = childWriters.sequencedValues()

    private val validityBuffer = ExtensibleBuffer(allocator)

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    override fun writeUndefined() {
        validityBuffer.writeBit(valueCount++, 0)
        childWriters.sequencedValues().forEach { it.writeUndefined() }
    }

    override val keys get() = childWriters.keys

    override fun keyReader(name: String) = childWriters[name]

    override fun keyWriter(name: String) = childWriters[name] ?: error("missing child vector: $name")

    override fun keyWriter(name: String, fieldType: FieldType) =
        childWriters.compute(name) { _, v ->
            if (v == null) {
                fromField(allocator, Field(name, fieldType, emptyList())).also { newVec ->
                    repeat(valueCount) { newVec.writeNull() }
                }
            } else {
                val existingFieldType = v.fieldType
                if (existingFieldType != fieldType) TODO("promotion to union")
                v
            }
        }!!

    override fun endStruct() {
        validityBuffer.writeBit(valueCount++, 1)

        val valueCount = valueCount

        childWriters.sequencedValues().forEach { child ->
            repeat(valueCount - child.valueCount) { child.writeNull() }
        }
    }

    override fun mapKeyReader(): VectorReader = childWriters.sequencedValues().firstOrNull() ?: TODO("auto-creation")
    override fun mapValueReader(): VectorReader = childWriters.sequencedValues().lastOrNull() ?: TODO("auto-creation")
    override fun mapKeyWriter(): VectorWriter = childWriters.sequencedValues().firstOrNull() ?: TODO("auto-creation")
    override fun mapValueWriter(): VectorWriter = childWriters.sequencedValues().lastOrNull() ?: TODO("auto-creation")

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Any =
        childWriters.sequencedEntrySet()
            .associateBy({ keyFn.denormalize(it.key) }, { it.value.getObject(idx, keyFn) })
            .filterValues { it != null }

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
                val childWriter = childWriters[key] ?: keyWriter(key, obj.toFieldType())

                if (childWriter.valueCount != this.valueCount)
                    throw xtdb.IllegalArgumentException(
                        "xtdb/key-already-set".asKeyword,
                        data = mapOf("ks".asKeyword to value.keys, "k".asKeyword to key)
                    )

                try {
                    childWriter.writeObject(obj)
                } catch (e: InvalidWriteObjectException) {
                    DenseUnionVector.promote(allocator, childWriter, e.obj.toFieldType())
                        .apply {
                            childWriters[key] = this
                            writeObject(obj)
                        }
                }
            }
            endStruct()
        }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val readers = childWriters.mapValues { it.value.valueReader(pos) }

        return object : ValueReader {
            override val isNull get() = this@StructVector.isNull(pos.position)

            override fun readObject() = if (isNull) null else readers
        }
    }

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) =
        childWriters.values.fold(0) { hash, child ->
            ByteFunctionHelpers.combineHash(hash, child.hashCode(idx, hasher))
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is StructVector)
        val childCopiers = src.childWriters.map { (childName, child) ->
            child.rowCopier(childWriters[childName] ?: error("missing child vector: $childName"))
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
        val node = nodes.removeFirst() ?: error("missing node")

        validityBuffer.loadBuffer(buffers.removeFirst() ?: error("missing validity buffer"))
        childWriters.sequencedValues().forEach { it.loadPage(nodes, buffers) }

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is NonNullableStructVector)
        validityBuffer.loadBuffer(vec.validityBuffer)
        childWriters.sequencedValues().forEach { it.loadFromArrow(vec.getChild(it.name)) }

        valueCount = vec.valueCount
    }

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