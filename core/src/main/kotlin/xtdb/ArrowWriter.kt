package xtdb

import xtdb.trie.FileSize

interface ArrowWriter : AutoCloseable {
    fun writePage()
    fun end(): FileSize
}
