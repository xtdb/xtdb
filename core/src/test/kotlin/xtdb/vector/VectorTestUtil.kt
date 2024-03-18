package xtdb.vector

internal fun IVectorWriter.toReader() = from(this.apply { syncValueCount() }.vector)
internal fun IVectorReader.toList() = List(valueCount()) { getObject(it) }
