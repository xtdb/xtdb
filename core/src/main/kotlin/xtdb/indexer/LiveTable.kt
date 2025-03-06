package xtdb.indexer

import com.google.protobuf.ByteString
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.roaringbitmap.RoaringBitmap
import xtdb.BufferPool
import xtdb.api.TransactionKey
import xtdb.arrow.VectorReader
import xtdb.bloom.bloomHashes
import xtdb.bloom.toByteBuffer
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.*
import xtdb.types.Fields
import xtdb.vector.*
import xtdb.log.proto.TrieMetadata
import xtdb.util.*
import java.nio.ByteBuffer
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

class LiveTable
@JvmOverloads constructor(
    al: BufferAllocator, bp: BufferPool,
    private val tableName: TableName,
    private val rowCounter: RowCounter,
    liveTrieFactory: LiveTrieFactory = LiveTrieFactory { MemoryHashTrie.emptyTrie(it) }
) : AutoCloseable {

    private val trieWriter = TrieWriter(al, bp, false)
    private val trieMetadataBuilder = TrieMetadata.newBuilder()
    private var iidBloom = RoaringBitmap()

    @FunctionalInterface
    fun interface LiveTrieFactory {
        operator fun invoke(iidWtr: IVectorReader): MemoryHashTrie
    }

    val liveRelation: IRelationWriter = Trie.openLogDataWriter(al)

    private val iidWtr = liveRelation.colWriter("_iid")
    private val systemFromWtr = liveRelation.colWriter("_system_from")
    private val validFromWtr = liveRelation.colWriter("_valid_from")
    private val validToWtr = liveRelation.colWriter("_valid_to")

    private val iidRdr = iidWtr.asReader
    private val systemFromRdr = systemFromWtr.asReader
    private val validFromRdr = validFromWtr.asReader
    private val validToRdr = validToWtr.asReader

    private val opWtr = liveRelation.colWriter("op")
    private val putWtr = opWtr.legWriter("put")
    private val deleteWtr = opWtr.legWriter("delete")
    private val eraseWtr = opWtr.legWriter("erase")

    var liveTrie: MemoryHashTrie = liveTrieFactory(iidWtr.vector.asReader)

    class Watermark(
        val columnFields: Map<String, Field>,
        val liveRelation: RelationReader,
        val liveTrie: MemoryHashTrie
    ) : AutoCloseable {
        fun columnField(col: String): Field = columnFields[col] ?: Fields.NULL.toArrowField(col)

        override fun close() {
            liveRelation.close()
        }
    }

    inner class Tx internal constructor(
        txKey: TransactionKey,
        private val newLiveTable: Boolean
    ) : AutoCloseable {
        private var transientTrie = liveTrie
        private val systemFrom: InstantMicros = txKey.systemTime.asMicros

        fun openWatermark(): Watermark = openWatermark(transientTrie)
        val docWriter: IVectorWriter = putWtr
        val liveRelation: IRelationWriter = this@LiveTable.liveRelation
        val startPos = liveRelation.writerPosition().position

        fun logPut(iid: ByteBuffer, validFrom: Long, validTo: Long, writeDocFun: Runnable) {
            val pos = liveRelation.writerPosition().position

            iidWtr.writeBytes(iid)
            systemFromWtr.writeLong(systemFrom)
            validFromWtr.writeLong(validFrom)
            validToWtr.writeLong(validTo)

            writeDocFun.run()

            liveRelation.endRow()

            transientTrie += pos
            rowCounter.addRows(1)
        }

        fun logDelete(iid: ByteBuffer, validFrom: Long, validTo: Long) {
            val pos = liveRelation.writerPosition().position

            iidWtr.writeBytes(iid)
            systemFromWtr.writeLong(systemFrom)
            validFromWtr.writeLong(validFrom)
            validToWtr.writeLong(validTo)
            deleteWtr.writeNull()
            liveRelation.endRow()

            transientTrie += pos
            rowCounter.addRows(1)
        }

        fun logErase(iid: ByteBuffer) {
            val pos = liveRelation.writerPosition().position

            iidWtr.writeBytes(iid)
            systemFromWtr.writeLong(systemFrom)
            validFromWtr.writeLong(MIN_LONG)
            validToWtr.writeLong(MAX_LONG)
            eraseWtr.writeNull()
            liveRelation.endRow()

            transientTrie += pos
            rowCounter.addRows(1)
        }

        fun commit() = this@LiveTable.also {
            val deltaBloom = RoaringBitmap()
            val pos = liveRelation.writerPosition().position
            for (i in startPos until pos) {
                trieMetadataBuilder.minValidFrom = minOf(trieMetadataBuilder.minValidFrom, validFromRdr.getLong(i))
                trieMetadataBuilder.maxValidFrom = maxOf(trieMetadataBuilder.maxValidFrom, validFromRdr.getLong(i))
                trieMetadataBuilder.minValidTo = minOf(trieMetadataBuilder.minValidTo , validToRdr.getLong(i))
                trieMetadataBuilder.maxValidTo = maxOf(trieMetadataBuilder.maxValidTo, validToRdr.getLong(i))
                trieMetadataBuilder.minSystemFrom = minOf(trieMetadataBuilder.minSystemFrom, systemFromRdr.getLong(i))
                trieMetadataBuilder.maxSystemFrom = maxOf(trieMetadataBuilder.maxSystemFrom, systemFromRdr.getLong(i))
                trieMetadataBuilder.rowCount += (pos - startPos)
                deltaBloom.add(*bloomHashes(VectorReader.from(iidRdr) , i))
            }
            it.liveTrie = transientTrie
            it.iidBloom = RoaringBitmap.or(it.iidBloom, deltaBloom)
        }

        fun abort() {
            if (newLiveTable) this@LiveTable.close()
        }

        override fun close() {
        }
    }

    fun startTx(txKey: TransactionKey, newLiveTable: Boolean) = Tx(txKey, newLiveTable)

    private val IRelationWriter.fields
        get() = colWriter("op").legWriter("put").field
            .also { assert(it.type is ArrowType.Struct) }
            .children
            .associateBy { it.name }

    private fun IRelationWriter.openWatermarkLiveRel(): RelationReader =
        mutableListOf<IVectorReader>().closeAllOnCatch { outCols ->
            for ((_, w) in this) {
                w.syncValueCount()
                outCols.add(w.vector.openSlice().asReader)
            }

            RelationReader.from(outCols)
        }

    private fun openWatermark(trie: MemoryHashTrie): Watermark {
        val wmLiveRel = liveRelation.openWatermarkLiveRel()
        val wmLiveTrie = trie.withIidReader(wmLiveRel.readerForName("_iid"))

        return Watermark(liveRelation.fields, wmLiveRel, wmLiveTrie)
    }

    fun openWatermark() = openWatermark(liveTrie)

    data class FinishedBlock(
        val fields: Map<String, Field>,
        val trieKey: TrieKey,
        val dataFileSize: FileSize,
        val rowCount: Int,
        val trieMetadata: TrieMetadata
    )

    fun finishBlock(blockIdx: BlockIndex): FinishedBlock? {
        liveRelation.syncRowCount()
        val rowCount = liveRelation.writerPosition().position
        if (rowCount == 0) return null
        val trieKey = Trie.l0Key(blockIdx).toString()
        trieMetadataBuilder.iidBloom  = ByteString.copyFrom(iidBloom.toByteBuffer().toByteArray())

        return liveRelation.openAsRelation().useAll { dataRel ->
            val dataFileSize = trieWriter.writeLiveTrie(tableName, trieKey, liveTrie, dataRel)
            FinishedBlock(liveRelation.fields, trieKey, dataFileSize, rowCount, trieMetadataBuilder.build())
        }
    }

    override fun close() {
        liveRelation.close()
    }
}