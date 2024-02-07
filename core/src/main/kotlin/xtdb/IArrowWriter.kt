package xtdb

interface IArrowWriter : AutoCloseable {
    fun writeBatch()
    fun end()
}
