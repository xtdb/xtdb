@file:JvmName("Iid")
package xtdb.util

import clojure.lang.Keyword
import xtdb.asBytes
import xtdb.api.error.Incorrect
import java.security.MessageDigest
import java.util.UUID

internal fun requireIid(bytes: ByteArray) {
    if (bytes.size != 16)
        throw Incorrect("an iid is 16 bytes, got ${bytes.size}", "xtdb/invalid-iid", mapOf("length" to bytes.size))
}

private val messageDigest = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

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