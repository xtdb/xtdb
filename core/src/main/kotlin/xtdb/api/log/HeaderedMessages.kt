package xtdb.api.log

import com.google.protobuf.CodedOutputStream
import com.google.protobuf.MessageLite

internal fun MessageLite.toHeaderedBytes(header: Byte): ByteArray =
    ByteArray(1 + serializedSize).also { bytes ->
        bytes[0] = header
        CodedOutputStream.newInstance(bytes, 1, bytes.size - 1).also { writeTo(it) }.checkNoSpaceLeft()
    }
