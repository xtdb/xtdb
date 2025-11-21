@file:JvmName("Iid")
package xtdb.util

import clojure.lang.Keyword
import xtdb.asBytes
import xtdb.error.Incorrect
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID

private val messageDigest = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

private fun UUID.toIidBytes(): ByteArray =
    ByteBuffer.allocate(16)
        .putLong(mostSignificantBits)
        .putLong(leastSignificantBits)
        .array()

val Any?.asIid: ByteArray
    get() = when (this) {
        is UUID -> this.asBytes
        else -> {
            val eidBytes = when (this) {
                is String -> "s$this".toByteArray()
                is Keyword -> "k$this".toByteArray()
                is Int, is Long, is Short, is Byte -> "i$this".toByteArray()
                else -> throw Incorrect(
                    "Invalid ID type: ${this?.javaClass?.name}",
                    "xtdb/invalid-id",
                    mapOf("type" to this?.javaClass?.name, "eid" to this)
                )
            }
            messageDigest.get().digest(eidBytes).copyOfRange(0, 16)
        }
    }