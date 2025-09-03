package xtdb.indexer

import com.google.protobuf.ByteString
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.bloom.bloomHashes
import xtdb.bloom.toByteBuffer
import xtdb.log.proto.TemporalMetadata
import xtdb.log.proto.TrieMetadata
import xtdb.util.toByteArray

class TrieMetadataCalculator(
    private val iidRdr: VectorReader,
    private val validFromRdr: VectorReader,
    private val validToRdr: VectorReader,
    private val systemFromRdr: VectorReader,
    private val recencyRdr: VectorReader?
) {

    private var rowCount = 0L
    private var iidBloom = RoaringBitmap()
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

                if (recencyRdr != null && !recencyRdr.isNull(i)) {
                    maxRecency = maxOf(maxRecency, recencyRdr.getLong(i))
                }
            }

            iidBloom.add(*bloomHashes(iidRdr, i))
        }
    }

    fun build(): TrieMetadata =
        TrieMetadata.newBuilder()
            .setTemporalMetadata(temporalBuilder.build())
            .setIidBloom(ByteString.copyFrom(iidBloom.toByteBuffer().toByteArray()))
            .setRowCount(rowCount)
            .build()
}