package xtdb.table

import xtdb.api.TableRef
import xtdb.trie.TrieKey
import xtdb.util.asPath
import java.nio.file.Path

/**
 * The directory under `tables/` holding one table's files.
 *
 * A table's location, never its identity: it is frozen when the table is first recorded and carried
 * forward verbatim from block to block, so renaming a table changes its name and leaves this alone.
 * Deriving one from a name is therefore only correct for a table that has no recorded slug — see [of].
 */
data class TableSlug(val slug: String) {

    val tablePath: Path get() = tablesDir.resolve(slug)

    fun dataFileDir(): Path = tablePath.resolve("data")

    fun dataFilePath(trieKey: TrieKey): Path = dataFileDir().resolve("$trieKey.arrow")

    fun metaFileDir(): Path = tablePath.resolve("meta")

    fun metaFilePath(trieKey: TrieKey): Path = metaFileDir().resolve("$trieKey.arrow")

    companion object {
        @JvmStatic
        val tablesDir = "tables".asPath

        /**
         * The slug a table takes when nothing has recorded one for it — whether because it is new, or
         * because it predates the registry.
         *
         * Those two cases must produce the same answer, which is why this stays the escaped name: a table
         * already on disk has to resolve to the path it has always had. Changing it orphans every file
         * written by a node that hasn't got the change yet, so it can only move at a release boundary at
         * which every node reads the recorded slug instead of calling this. See #4037.
         */
        @JvmStatic
        fun of(table: TableRef) =
            TableSlug("${table.schemaName}$${table.tableName}".replace(Regex("[./]"), "\\$"))
    }
}
