package xtdb.table

import xtdb.trie.TrieKey
import java.nio.file.Path
import java.util.UUID

/** One secondary index's files, under its table's directory: `tables/<slug>/indices/<index-id>/{data,meta}/<key>.arrow`. */
data class IndexFiles(val table: TableSlug, val indexId: UUID) : TrieFiles {
    private val indexPath: Path get() = table.tablePath.resolve("indices").resolve(indexId.toString())

    override fun dataFilePath(trieKey: TrieKey): Path = indexPath.resolve("data").resolve("$trieKey.arrow")

    override fun metaFilePath(trieKey: TrieKey): Path = indexPath.resolve("meta").resolve("$trieKey.arrow")
}
