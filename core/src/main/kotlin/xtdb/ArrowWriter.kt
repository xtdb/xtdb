package xtdb

import xtdb.trie.FileSize

interface ArrowWriter : AutoCloseable {
    fun writeBatch()
    fun end(): FileSize
}
