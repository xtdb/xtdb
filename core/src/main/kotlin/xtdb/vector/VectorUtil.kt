package xtdb.vector

import org.apache.arrow.vector.FixedSizeBinaryVector
import java.nio.ByteBuffer

internal fun FixedSizeBinaryVector.setBytes(idx: Int, bytes: ByteBuffer) {
    setIndexDefined(idx)
    dataBuffer.setBytes((idx * byteWidth).toLong(), bytes)
}