@file:JvmName("Iid")
package xtdb.util

import clojure.lang.Keyword
import xtdb.asBytes
import xtdb.api.error.Incorrect
import java.security.MessageDigest
import java.util.UUID

private val messageDigest = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

/** [asIid], or null for a value of a type that can't key a row. */
val Any?.asIidOrNull: ByteArray?
    get() = when (this) {
        is UUID -> this.asBytes
        is String -> "s$this".sha256Iid()
        is Keyword -> "k$this".sha256Iid()
        is Int, is Long, is Short, is Byte -> "i$this".sha256Iid()
        else -> null
    }

private fun String.sha256Iid(): ByteArray = messageDigest.get().digest(toByteArray()).copyOfRange(0, 16)

val Any?.asIid: ByteArray
    get() = asIidOrNull ?: throw Incorrect(
        "Invalid ID type: ${this?.javaClass?.name}",
        "xtdb/invalid-id",
        mapOf("type" to this?.javaClass?.name, "eid" to this)
    )