package xtdb.table

import clojure.lang.Symbol
import xtdb.trie.Trie.tablesDir
import xtdb.trie.TrieKey
import xtdb.util.requiringResolve
import java.nio.file.Path

typealias SchemaName = String
typealias TableName = String

private val TABLE_REF_REGEX = Regex("([^/]+)/([^/]+)")

interface TableRef {
    val schemaName: SchemaName
    val tableName: TableName

    val sym: Symbol get() = Symbol.intern(schemaName, tableName)

    companion object {
        @JvmStatic
        fun parse(str: String): TableRef {
            val match = (TABLE_REF_REGEX.matchEntire(str) ?: error("Invalid table-ref: `$str`")).groupValues

            return tableRef(match[1], match[2])
        }

        @JvmStatic
        val TableRef.tablePath: Path
            get() =
                tablesDir.resolve("$schemaName$$tableName".replace(Regex("[./]"), "\\$"))

        @JvmStatic
        fun TableRef.dataFilePath(trieKey: TrieKey): Path =
            tablePath.resolve("data").resolve("$trieKey.arrow")

        @JvmStatic
        fun TableRef.metaFileDir(): Path = tablePath.resolve("meta")

        @JvmStatic
        fun TableRef.metaFilePath(trieKey: TrieKey): Path =
            metaFileDir().resolve("$trieKey.arrow")
    }
}

fun tableRef(schema: SchemaName = "public", table: TableName) =
    requiringResolve("xtdb.table/->ref")(schema, table) as TableRef
