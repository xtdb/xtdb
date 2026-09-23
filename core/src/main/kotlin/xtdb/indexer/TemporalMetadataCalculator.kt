package xtdb.indexer

import xtdb.arrow.VectorReader
import xtdb.log.proto.TemporalMetadata

private inline fun setMinMax(vec: VectorReader, set: (Long, Long) -> Unit) {
    var min = Long.MAX_VALUE
    var max = Long.MIN_VALUE

    for (i in 0..<vec.valueCount) {
        val v = vec.getLong(i)
        min = minOf(min, v)
        max = maxOf(max, v)
    }

    set(min, max)
}

fun TemporalMetadata.Builder.update(validFrom: VectorReader, validTo: VectorReader, systemFrom: VectorReader) = apply {
    setMinMax(validFrom) { min, max ->
        minValidFrom = minOf(minValidFrom, min)
        maxValidFrom = maxOf(maxValidFrom, max)
    }

    setMinMax(validTo) { min, max ->
        minValidTo = minOf(minValidTo, min)
        maxValidTo = maxOf(maxValidTo, max)
    }

    setMinMax(systemFrom) { min, max ->
        minSystemFrom = minOf(minSystemFrom, min)
        maxSystemFrom = maxOf(maxSystemFrom, max)
    }
}