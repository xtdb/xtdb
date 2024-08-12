package xtdb.arrow

interface RelationUnloader : AutoCloseable {
    fun writeBatch()
    fun endStream()
    fun endFile()

}