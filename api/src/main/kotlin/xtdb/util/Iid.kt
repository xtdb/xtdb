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

/**
 * The IID of the row a primary key's [keyValues] identify, in key order.
 * A single value's IID is its own [asIid]; several are combined by [compositeIid].
 * Throws `xtdb/invalid-id` for an empty list, or for a value of a type that can't key a row.
 */
fun keyIid(keyValues: List<*>): ByteArray =
    when (keyValues.size) {
        0 -> throw Incorrect("A key has at least one value", "xtdb/invalid-id", mapOf("eid" to keyValues))
        1 -> keyValues.single().asIid
        else -> compositeIid(keyValues.map { it.asIid })
    }

/**
 * [keyIid] of a row's key as `delete-docs` and `erase-docs` take it: a [List] holds a key's values in key order, anything else is a single-column key's value.
 * Null for an empty list, or for a value of a type that can't key a row — a list among them.
 */
fun rowKeyIidOrNull(rowKey: Any?): ByteArray? =
    when (rowKey) {
        is List<*> ->
            if (rowKey.isEmpty()) null
            else rowKey.map { it.asIidOrNull ?: return null }.let { it.singleOrNull() ?: compositeIid(it) }

        else -> rowKey.asIidOrNull
    }

/** [rowKeyIidOrNull], throwing `xtdb/invalid-id` where that is null. */
fun rowKeyIid(rowKey: Any?): ByteArray =
    rowKeyIidOrNull(rowKey) ?: throw Incorrect(
        "Invalid row key: $rowKey",
        "xtdb/invalid-id",
        mapOf("type" to rowKey?.javaClass?.name, "eid" to rowKey)
    )

/**
 * The bits of an IID's 32-bit prefix that column [col] of an [n]-column key owns, big-endian:
 * nibble k (0..7) belongs to column k mod n.
 */
fun prefixMask(n: Int, col: Int): Int {
    require(n >= 1 && col in 0 until n) { "column $col of a $n-column key" }
    var mask = 0
    for (k in col until 8 step n) mask = mask or (0xF shl (28 - 4 * k))
    return mask
}

private fun ByteArray.iidPrefix(): Int =
    ((this[0].toInt() and 0xFF) shl 24) or ((this[1].toInt() and 0xFF) shl 16) or
        ((this[2].toInt() and 0xFF) shl 8) or (this[3].toInt() and 0xFF)

/**
 * The IID of a row keyed on n >= 2 columns, from its columns' IIDs in key order.
 *
 * - The 32-bit prefix is the OR of each column's IID masked by [prefixMask], so an IID's bits under column c's mask are column c's.
 * - The low 96 bits are the first 12 bytes of a SHA-256 over the columns' IIDs, concatenated.
 */
fun compositeIid(colIids: List<ByteArray>): ByteArray {
    val n = colIids.size
    require(n >= 2) { "a composite IID needs at least two columns" }
    require(colIids.all { it.size == 16 }) { "an IID is 16 bytes" }

    val md = messageDigest.get()
    colIids.forEach(md::update)
    val digest = md.digest()

    var prefix = 0
    for (c in 0 until n) prefix = prefix or (colIids[c].iidPrefix() and prefixMask(n, c))

    return ByteArray(16).also { out ->
        for (b in 0 until 4) out[b] = (prefix ushr (24 - 8 * b)).toByte()
        digest.copyInto(out, destinationOffset = 4, startIndex = 0, endIndex = 12)
    }
}
