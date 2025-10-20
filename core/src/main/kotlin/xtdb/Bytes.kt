package xtdb

import java.util.Arrays

class Bytes(bytes: ByteArray) : Comparable<Bytes> {
    val bytes: ByteArray = bytes
        get() = field.clone()

    override fun equals(other: Any?) = other is Bytes && bytes.contentEquals(other.bytes)
    override fun hashCode() = bytes.contentHashCode()
    override fun toString() = """(Bytes "${bytes.toHexString()}")"""

    override operator fun compareTo(other: Bytes) = Arrays.compareUnsigned(bytes, other.bytes)

    companion object {
        @JvmField
        val COMPARATOR: java.util.Comparator<ByteArray> = Comparator { l, r ->
            Arrays.compareUnsigned(l, r)
        }
    }
}