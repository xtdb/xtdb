package xtdb

interface ArrowWriter : AutoCloseable {
    fun writeBatch()
    fun end()
}
