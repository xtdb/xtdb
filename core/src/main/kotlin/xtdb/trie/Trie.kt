package xtdb.trie

import com.carrotsearch.hppc.ByteArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.types.Fields
import xtdb.types.NamelessField
import xtdb.types.Schema
import xtdb.util.StringUtil.asLexHex
import xtdb.util.StringUtil.fromLexHex
import xtdb.util.asPath
import xtdb.util.closeOnCatch
import xtdb.vector.IRelationWriter
import xtdb.vector.RootWriter
import java.nio.file.Path
import java.time.LocalDate
import java.time.format.DateTimeFormatterBuilder
import java.time.format.SignStyle
import java.time.temporal.ChronoField

typealias Level = Long
typealias InstantMicros = Long
typealias RowIndex = Int
typealias BlockIndex = Long
typealias TableName = String
typealias ColumnName = String
typealias TrieKey = String

object Trie {
    internal val RECENCY_FMT = DateTimeFormatterBuilder()
        .parseLenient()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.NOT_NEGATIVE)
        .appendValue(ChronoField.MONTH_OF_YEAR, 2)
        .appendValue(ChronoField.DAY_OF_MONTH, 2)
        .toFormatter()

    data class Key(val level: Level, val recency: LocalDate?, val part: ByteArrayList?, val blockIndex: BlockIndex) {
        override fun toString() = buildString {
            append("l"); append(level.asLexHex)
            append("-r"); append(recency?.let { RECENCY_FMT.format(it) } ?: "c")
            part?.takeIf { it.size() > 0 }?.let { append("-p"); it.forEach { b -> append(b.value) } }
            append("-b"); append(blockIndex.asLexHex)
        }
    }

    private val String.asPart
        get() = ByteArrayList.from(*ByteArray(length) { this[it].digitToInt(4).toByte() })

    fun l0Key(blockIndex: BlockIndex) = Key(0, null, null, blockIndex)

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

    @JvmStatic
    val tablesDir = "tables".asPath

    @JvmStatic
    val TableName.tablePath: Path get() = tablesDir.resolve(replace(Regex("[./]"), "\\$"))

    @JvmStatic
    fun TableName.dataFilePath(trieKey: TrieKey): Path =
        tablePath.resolve("data").resolve("$trieKey.arrow")

    @JvmStatic
    fun TableName.metaFileDir(): Path = tablePath.resolve("meta")

    @JvmStatic
    fun TableName.metaFilePath(trieKey: TrieKey): Path =
        metaFileDir().resolve("$trieKey.arrow")

    @JvmStatic
    fun dataRelSchema(putDocField: Field): Schema =
        Schema(
            "_iid" to Fields.IID,
            "_system_from" to Fields.TEMPORAL,
            "_valid_from" to Fields.TEMPORAL,
            "_valid_to" to Fields.TEMPORAL,
            "op" to Fields.Union(
                "put" to NamelessField(putDocField.fieldType, putDocField.children),
                "delete" to Fields.NULL,
                "erase" to Fields.NULL
            )
        )

    private fun dataRelSchema(putDocField: NamelessField) = dataRelSchema(putDocField.toArrowField("put"))

    @JvmStatic
    fun openLogDataWriter(
        allocator: BufferAllocator,
        dataSchema: Schema = dataRelSchema(Fields.Struct())
    ): IRelationWriter =
        VectorSchemaRoot.create(dataSchema, allocator).closeOnCatch { root -> RootWriter(root) }
}