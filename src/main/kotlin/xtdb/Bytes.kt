package xtdb

class Bytes(bytes: ByteArray) {
    val bytes: ByteArray = bytes
        get() = field.clone()

    override fun equals(other: Any?) = other is Bytes && bytes.contentEquals(other.bytes)
    override fun hashCode() = bytes.contentHashCode()
    override fun toString() = """(Bytes "${bytes.toHexString()}")"""
}