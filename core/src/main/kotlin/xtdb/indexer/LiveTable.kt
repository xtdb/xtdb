package xtdb.indexer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.arrow.*
import xtdb.arrow.VectorType.*
import xtdb.log.proto.TrieMetadata
import xtdb.segment.MemorySegment
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.*
import xtdb.util.HLL
import xtdb.util.RowCounter
import xtdb.util.closeOnCatch
import java.nio.ByteBuffer
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

class LiveTable
@JvmOverloads
constructor(
    private val al: BufferAllocator, bp: BufferPool,
    private val table: TableRef,
    private val rowCounter: RowCounter,
    liveTrieFactory: LiveTrieFactory = LiveTrieFactory { MemoryHashTrie.emptyTrie(it) }
) : AutoCloseable {

    @FunctionalInterface
    fun interface LiveTrieFactory {
        operator fun invoke(iidVec: VectorReader): MemoryHashTrie
    }

    val liveRelation: Relation = Trie.openLogDataWriter(al)

    private val iidVec = liveRelation["_iid"]
    private val systemFromVec = liveRelation["_system_from"]
    private val validFromVec = liveRelation["_valid_from"]
    private val validToVec = liveRelation["_valid_to"]

    var liveTrie: MemoryHashTrie = liveTrieFactory(iidVec)

    private val opVec = liveRelation["op"]
    private val putVec by lazy { opVec.vectorFor("put", STRUCT_TYPE, false) }
    private val deleteVec = opVec["delete"]
    private val eraseVec = opVec["erase"]

    private val trieWriter = LiveTrieWriter(al, bp, calculateBlooms = false)
    private val trieMetadataCalculator = TrieMetadataCalculator(
        iidVec, validFromVec, validToVec, systemFromVec
    )

    private val hllCalculator = HllCalculator()

    class Snapshot(
        val columnTypes: Map<ColumnName, VectorType>,
        val segment: MemorySegment
    ) : AutoCloseable {
        val liveRelation: RelationReader get() = segment.rel
        val liveTrie: MemoryHashTrie get() = segment.trie

        fun columnType(col: ColumnName): VectorType = columnTypes[col] ?: Null

        val types: Map<ColumnName, VectorType> get() = columnTypes

        override fun close() {
            segment.rel.close()
        }
    }

    inner class Tx internal constructor(
        txKey: TransactionKey,
        private val newLiveTable: Boolean
    ) : AutoCloseable {
        var transientTrie = liveTrie; private set
        private val systemFrom: InstantMicros = txKey.systemTime.asMicros

        fun openSnapshot(): Snapshot = openSnapshot(transientTrie)
        val docWriter: VectorWriter by lazy { putVec }
        val liveRelation: RelationWriter = this@LiveTable.liveRelation

        val startPos = liveRelation.rowCount

        fun logPut(iid: ByteBuffer, validFrom: Long, validTo: Long, writeDocFun: Runnable) {
            val pos = liveRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(validFrom)
            validToVec.writeLong(validTo)

            writeDocFun.run()

            liveRelation.endRow()

            transientTrie += pos
            rowCounter.addRows(1)
        }

        fun logDelete(iid: ByteBuffer, validFrom: Long, validTo: Long) {
            val pos = liveRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(validFrom)
            validToVec.writeLong(validTo)
            deleteVec.writeNull()
            liveRelation.endRow()

            transientTrie += pos
            rowCounter.addRows(1)
        }

        fun logErase(iid: ByteBuffer) {
            val pos = liveRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(MIN_LONG)
            validToVec.writeLong(MAX_LONG)
            eraseVec.writeNull()
            liveRelation.endRow()

            transientTrie += pos
            rowCounter.addRows(1)
        }

        fun commit(): LiveTable {
            val pos = liveRelation.rowCount
            trieMetadataCalculator.update(startPos, pos)
            hllCalculator.update(opVec, startPos, pos)

            liveTrie = transientTrie

            return this@LiveTable
        }

        fun abort() {
            if (newLiveTable) this@LiveTable.close()
        }

        override fun close() {
        }
    }

    fun startTx(txKey: TransactionKey, newLiveTable: Boolean) = Tx(txKey, newLiveTable)

    private val RelationWriter.types: Map<String, VectorType>
        get() {
            val putVec = vectorFor("op").vectorForOrNull("put") ?: return emptyMap()
            val type = putVec.type
            check(type is Mono && type.arrowType == STRUCT_TYPE) {
                "Expected 'put' vector to be STRUCT type, got: $type"
            }
            return type.children
        }

    private fun openSnapshot(trie: MemoryHashTrie): Snapshot {
        liveRelation.openDirectSlice(al).closeOnCatch { wmLiveRel ->
            val wmLiveTrie = trie.withIidReader(wmLiveRel["_iid"])

            return Snapshot(liveRelation.types, MemorySegment(wmLiveTrie, wmLiveRel))
        }
    }

    fun openSnapshot() = openSnapshot(liveTrie)

    data class FinishedBlock(
        val types: Map<FieldName, VectorType>,
        val trieKey: TrieKey,
        val dataFileSize: FileSize,
        val rowCount: Int,
        val trieMetadata: TrieMetadata,
        val hllDeltas: Map<FieldName, HLL>
    )

    fun finishBlock(blockIdx: BlockIndex): FinishedBlock? {
        val rowCount = liveRelation.rowCount
        if (rowCount == 0) return null
        val trieKey = Trie.l0Key(blockIdx).toString()

        return liveRelation.openDirectSlice(al).use { dataRel ->
            val dataFileSize = trieWriter.writeLiveTrie(table, trieKey, liveTrie, dataRel)
            FinishedBlock(
                liveRelation.types, trieKey, dataFileSize, rowCount,
                trieMetadataCalculator.build(), hllCalculator.build()
            )
        }
    }

    companion object {
        @JvmStatic
        fun Map<TableRef, LiveTable>.finishBlock(blockIdx: BlockIndex): Map<TableRef, FinishedBlock> =
            // migrated here because of #5107 - may migrate the rest later.
            runBlocking {
                this@finishBlock
                    .map { (tableName, liveTable) ->
                        async(Dispatchers.IO) {
                            tableName to liveTable.finishBlock(blockIdx)
                        }
                    }
                    .awaitAll()
                    .mapNotNull { (name, block) -> block?.let { name to it } }
                    .toMap()
            }
    }

    override fun close() {
        liveRelation.close()
    }
}
