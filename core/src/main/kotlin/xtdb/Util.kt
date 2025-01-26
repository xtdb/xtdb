package xtdb

import java.nio.ByteBuffer
import java.util.*

val UUID.asByteBuffer: ByteBuffer
    get() = ByteBuffer.wrap(ByteArray(16)).apply {
        putLong(mostSignificantBits)
        putLong(leastSignificantBits)
        flip()
    }

val UUID.asBytes: ByteArray get() = asByteBuffer.array()