package xtdb.util

import java.nio.ByteBuffer

fun ByteBuffer.toByteArray(): ByteArray {
    return if (hasArray() && arrayOffset() == 0 && array().size == remaining()) {
        array()
    } else {
        ByteArray(remaining()).also { get(it) }
    }
}