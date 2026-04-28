package xtdb.indexer

import xtdb.arrow.VectorReader
import xtdb.log.proto.TemporalMetadata
import xtdb.log.proto.TrieMetadata

class TrieMetadataCalculator(
    private val validFromRdr: VectorReader,
    private val validToRdr: VectorReader,
    private val systemFromRdr: VectorReader
) {

    private var rowCount = 0L
    private val temporalBuilder = TemporalMetadata.newBuilder()

    fun update(startPos: Int, endPos: Int) {
        rowCount += endPos - startPos

        for (i in startPos..<endPos) {
            temporalBuilder.apply {
                val validFrom = validFromRdr.getLong(i)
                minValidFrom = minOf(minValidFrom, validFrom)
                maxValidFrom = maxOf(maxValidFrom, validFrom)

                val validTo = validToRdr.getLong(i)
                minValidTo = minOf(minValidTo, validTo)
                maxValidTo = maxOf(maxValidTo, validTo)

                val systemFrom = systemFromRdr.getLong(i)
                minSystemFrom = minOf(minSystemFrom, systemFrom)
                maxSystemFrom = maxOf(maxSystemFrom, systemFrom)
            }
        }
    }

    fun build(): TrieMetadata =
        TrieMetadata.newBuilder()
            .setTemporalMetadata(temporalBuilder.build())
            .setRowCount(rowCount)
            .build()
}