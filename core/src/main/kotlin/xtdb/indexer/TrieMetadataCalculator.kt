package xtdb.indexer

import com.google.protobuf.ByteString
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.bloom.bloomHashes
import xtdb.bloom.toByteBuffer
import xtdb.log.proto.TemporalMetadata
import xtdb.log.proto.TrieMetadata
import xtdb.util.toByteArray
import xtdb.vector.IVectorReader

class TrieMetadataCalculator(
    private val iidRdr: VectorReader,
    private val validFromRdr: IVectorReader,
    private val validToRdr: IVectorReader,
    private val systemFromRdr: IVectorReader
) {

    private var rowCount = 0L
    private var iidBloom = RoaringBitmap()
    private val temporalBuilder = TemporalMetadata.newBuilder()

    fun update(startPos: Int, endPos: Int) {
        rowCount += endPos - startPos

        for (i in startPos..<endPos) {
            temporalBuilder.minValidFrom = minOf(temporalBuilder.minValidFrom, validFromRdr.getLong(i))
            temporalBuilder.maxValidFrom = maxOf(temporalBuilder.maxValidFrom, validFromRdr.getLong(i))

            temporalBuilder.minValidTo = minOf(temporalBuilder.minValidTo, validToRdr.getLong(i))
            temporalBuilder.maxValidTo = maxOf(temporalBuilder.maxValidTo, validToRdr.getLong(i))

            temporalBuilder.minSystemFrom = minOf(temporalBuilder.minSystemFrom, systemFromRdr.getLong(i))
            temporalBuilder.maxSystemFrom = maxOf(temporalBuilder.maxSystemFrom, systemFromRdr.getLong(i))

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