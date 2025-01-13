package xtdb.vector

internal fun IVectorWriter.toReader() = from(this.apply { syncValueCount() }.vector)
