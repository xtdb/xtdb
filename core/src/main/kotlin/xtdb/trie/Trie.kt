package xtdb.trie

import com.carrotsearch.hppc.ByteArrayList
import xtdb.util.StringUtil.asLexHex
import xtdb.util.StringUtil.fromLexHex
import java.time.LocalDate
import java.time.format.DateTimeFormatterBuilder

typealias Level = Long
typealias InstantMicros = Long
typealias RowIndex = Int
typealias BlockIndex = Long
typealias TableName = String
typealias TrieKey = String

object Trie {
    private val RECENCY_FMT = DateTimeFormatterBuilder().appendPattern("yyyyMMdd").toFormatter()

    data class Key(val level: Level, val recency: LocalDate?, val part: ByteArrayList?, val blockIndex: BlockIndex) {
        override fun toString() = buildString {
            append("l"); append(level.asLexHex)
            append("-r"); append(recency?.let { RECENCY_FMT.format(it) } ?: "c")
            part?.let { append("-p"); it.forEach { b -> append(b.value)} }
            append("-b"); append(blockIndex.asLexHex)
        }
    }

    private val String.asPart
        get() = ByteArrayList.from(*ByteArray(length) { this[it].digitToInt(4).toByte() })

    @JvmStatic
    fun parseKey(trieKey: TrieKey): Key {
        var level: Level? = null
        var recency: LocalDate? = null
        var part: ByteArrayList? = null
        var blockIndex: BlockIndex? = null

        trieKey.split("-").forEach { seg ->
            val arg = seg.substring(1)
            when (seg[0]) {
                'l' -> level = arg.fromLexHex
                'r' -> recency = arg.takeUnless { it == "c" }?.let { LocalDate.parse(it, RECENCY_FMT) }
                'p' -> part = arg.asPart
                'b' -> blockIndex = arg.fromLexHex
                else -> error("Invalid trie key: $trieKey")
            }
        }

        return Key(
            requireNotNull(level) { "Invalid trie key: $trieKey" },
            recency, part,
            requireNotNull(blockIndex) { "Invalid trie key: $trieKey" }
        )
    }
}