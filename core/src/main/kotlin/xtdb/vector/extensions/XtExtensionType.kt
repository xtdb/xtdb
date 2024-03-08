package xtdb.vector.extensions

import org.apache.arrow.vector.types.pojo.ArrowType

abstract class XtExtensionType protected constructor(
    private val extensionName: String,
    private val storageType: ArrowType,
) : ArrowType.ExtensionType() {

    override fun storageType() = storageType
    override fun extensionName() = extensionName
    override fun extensionEquals(other: ExtensionType) = javaClass.isAssignableFrom(other.javaClass)

    override fun serialize() = ""

    override fun deserialize(storageType: ArrowType, serializedData: String): ArrowType {
        if (storageType != this.storageType)
            throw UnsupportedOperationException("Cannot construct '${javaClass.simpleName}' from underlying type $storageType")

        return deserialize(serializedData)
    }

    protected abstract fun deserialize(serializedData: String): ArrowType
}
