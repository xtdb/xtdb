package xtdb.metadata

interface PageMetadataWriter {
    fun writeMetadata(cols: Iterable<*>)
}