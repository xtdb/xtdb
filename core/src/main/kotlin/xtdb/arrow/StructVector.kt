package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ByteFunctionHelpers
import org.apache.arrow.memory.util.hash.ArrowBufHasher
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.NonNullableStructVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import java.util.*
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE

class StructVector(
    private val allocator: BufferAllocator,
    override val name: String,
    override val fieldType: FieldType,
    private val childrenByKey: SequencedMap<String, Vector>,
) : Vector() {

    constructor(allocator: BufferAllocator, name: String, nullable: Boolean, childrenByKey: SequencedMap<String, Vector> = LinkedHashMap())
            : this(allocator, name, FieldType(nullable, STRUCT_TYPE, null), childrenByKey)

    override val children: Iterable<Vector> get() = childrenByKey.sequencedValues()

    private val validityBuffer = ExtensibleBuffer(allocator)

    override fun isNull(idx: Int) = !validityBuffer.getBit(idx)

    override fun writeNull() {
        validityBuffer.writeBit(valueCount++, 0)
        childrenByKey.sequencedValues().forEach { it.writeUndefined() }
    }

    override val keys get() = childrenByKey.keys

    override fun keyReader(name: String) = childrenByKey[name]

    override fun keyWriter(name: String) = childrenByKey[name] ?: TODO("auto-creation")

    override fun keyWriter(name: String, fieldType: FieldType) =
        childrenByKey.compute(name) { _, v ->
            if (v == null) {
                fromField(allocator, Field(name, fieldType, emptyList())).also { newVec ->
                    repeat(valueCount) { newVec.writeNull() }
                }
            } else {
                val existingFieldType = v.field.fieldType
                if (existingFieldType != fieldType) TODO("promotion to union")
                v
            }
        }!!

    override fun endStruct() {
        validityBuffer.writeBit(valueCount++, 1)

        val valueCount = valueCount

        childrenByKey.sequencedValues().forEach { child ->
            repeat(valueCount - child.valueCount) { child.writeUndefined() }
        }
    }

    override fun mapKeyReader(): VectorReader = childrenByKey.sequencedValues().firstOrNull() ?: TODO("auto-creation")
    override fun mapValueReader(): VectorReader = childrenByKey.sequencedValues().lastOrNull() ?: TODO("auto-creation")
    override fun mapKeyWriter(): VectorWriter = childrenByKey.sequencedValues().firstOrNull() ?: TODO("auto-creation")
    override fun mapValueWriter(): VectorWriter = childrenByKey.sequencedValues().lastOrNull() ?: TODO("auto-creation")

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): Any =
        childrenByKey.sequencedEntrySet()
            .associateBy({ keyFn.denormalize(it.key) }, { it.value.getObject(idx, keyFn) })
            .filterValues { it != null }

    override fun writeObject0(value: Any) =
        if (value !is Map<*, *>) TODO("unknown type: ${value::class.simpleName}")
        else {
            value.forEach {
                (childrenByKey[it.key] ?: TODO("promotion not supported yet")).writeObject(it.value)
            }
            endStruct()
        }

    override fun valueReader(pos: VectorPosition): ValueReader {
        val readers = childrenByKey.mapValues { it.value.valueReader(pos) }

        return object : ValueReader {
            override val isNull get() = this@StructVector.isNull(pos.position)

            override fun readObject() = if (isNull) null else readers
        }
    }

    override fun hashCode0(idx: Int, hasher: ArrowBufHasher) =
        childrenByKey.values.fold(0) { hash, child ->
            ByteFunctionHelpers.combineHash(hash, child.hashCode(idx, hasher))
        }

    override fun rowCopier0(src: VectorReader): RowCopier {
        require(src is StructVector)
        val childCopiers = src.childrenByKey.map { (childName, child) ->
            child.rowCopier(childrenByKey[childName] ?: error("missing child vector: $childName"))
        }

        return RowCopier { srcIdx ->
            childCopiers.forEach { it.copyRow(srcIdx) }
            valueCount.also { endStruct() }
        }
    }

    override fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), -1))
        validityBuffer.unloadBuffer(buffers)

        childrenByKey.sequencedValues().forEach { it.unloadBatch(nodes, buffers) }
    }

    override fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirst() ?: error("missing node")

        validityBuffer.loadBuffer(buffers.removeFirst() ?: error("missing validity buffer"))
        childrenByKey.sequencedValues().forEach { it.loadBatch(nodes, buffers) }

        valueCount = node.length
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is NonNullableStructVector)
        validityBuffer.loadBuffer(vec.validityBuffer)
        childrenByKey.sequencedValues().forEach { it.loadFromArrow(vec.getChild(it.name)) }

        valueCount = vec.valueCount
    }

    override fun clear() {
        validityBuffer.clear()
        valueCount = 0
        childrenByKey.sequencedValues().forEach(Vector::clear)
    }

    override fun close() {
        validityBuffer.close()
        childrenByKey.sequencedValues().forEach(Vector::close)
    }
}