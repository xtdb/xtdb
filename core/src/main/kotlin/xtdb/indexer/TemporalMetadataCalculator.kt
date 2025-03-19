package xtdb.indexer

import xtdb.log.proto.TemporalMetadata
import xtdb.vector.IVectorReader

class TemporalMetadataCalculator(
    private val validFromRdr: IVectorReader,
    private val validToRdr: IVectorReader,
    private val systemFromRdr: IVectorReader
) {
    private val builder = TemporalMetadata.newBuilder()

    fun update(startPos: Int, endPos: Int) {
        for (i in startPos..<endPos) {
            builder.minValidFrom = minOf(builder.minValidFrom, validFromRdr.getLong(i))
            builder.maxValidFrom = maxOf(builder.maxValidFrom, validFromRdr.getLong(i))

            builder.minValidTo = minOf(builder.minValidTo, validToRdr.getLong(i))
            builder.maxValidTo = maxOf(builder.maxValidTo, validToRdr.getLong(i))

            builder.minSystemFrom = minOf(builder.minSystemFrom, systemFromRdr.getLong(i))
            builder.maxSystemFrom = maxOf(builder.maxSystemFrom, systemFromRdr.getLong(i))
        }
    }

    fun build(): TemporalMetadata = builder.build()
}