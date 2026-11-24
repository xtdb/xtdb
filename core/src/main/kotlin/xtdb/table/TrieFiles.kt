package xtdb.table

import xtdb.trie.TrieKey
import java.nio.file.Path

/** The object-store paths of a set of tries' files: one data file and one meta file per trie key. */
interface TrieFiles {
    fun dataFilePath(trieKey: TrieKey): Path
    fun metaFilePath(trieKey: TrieKey): Path
}
