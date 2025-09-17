package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.TransactionKey
import xtdb.arrow.*
import xtdb.log.proto.TrieMetadata
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.*
import xtdb.types.Type
import xtdb.types.Type.Companion.ofType
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
    private val putVec by lazy { opVec.vectorFor("put", Type.structOf().fieldType) }
    private val deleteVec = opVec["delete"]
    private val eraseVec = opVec["erase"]

    private val trieWriter = LiveTrieWriter(al, bp, calculateBlooms = false)
    private val trieMetadataCalculator = TrieMetadataCalculator(
        iidVec, validFromVec, validToVec, systemFromVec
    )

    private val hllCalculator = HllCalculator()

    class Snapshot(
        val columnFields: Map<ColumnName, Field>,
        val liveRelation: RelationReader,
        val liveTrie: MemoryHashTrie
    ) : AutoCloseable {
        fun columnField(col: ColumnName): Field = columnFields[col] ?: col ofType Type.NULL

        override fun close() {
            liveRelation.close()
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

        private val startPos = liveRelation.rowCount

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
            hllCalculator.update(opVec.asReader, startPos, pos)

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

    private val RelationWriter.fields: Map<String, Field>
        get() {
            val putVec = vectorFor("op").vectorForOrNull("put") ?: return emptyMap()
            return putVec.field
                .also { assert(it.type is ArrowType.Struct) }
                .children
                .associateBy { it.name }
        }

    private fun openSnapshot(trie: MemoryHashTrie): Snapshot {
        // this can be openSlice once scan et al use new-style vectors
        liveRelation.openDirectSlice(al).use { wmLiveRel ->
            wmLiveRel.openAsRoot(al).closeOnCatch { root ->
                val relReader = RelationReader.from(root)

                val wmLiveTrie = trie.withIidReader(relReader["_iid"])

                return Snapshot(liveRelation.fields, relReader, wmLiveTrie)
            }
        }
    }

    fun openSnapshot() = openSnapshot(liveTrie)

    data class FinishedBlock(
        val fields: Map<ColumnName, Field>,
        val trieKey: TrieKey,
        val dataFileSize: FileSize,
        val rowCount: Int,
        val trieMetadata: TrieMetadata,
        val hllDeltas: Map<ColumnName, HLL>
    )

    fun finishBlock(blockIdx: BlockIndex): FinishedBlock? {
        liveRelation.vectors.forEach { it.asReader }
        val rowCount = liveRelation.rowCount
        if (rowCount == 0) return null
        val trieKey = Trie.l0Key(blockIdx).toString()

        return liveRelation.openDirectSlice(al).use { dataRel ->
            val dataFileSize = trieWriter.writeLiveTrie(table, trieKey, liveTrie, dataRel)
            FinishedBlock(
                liveRelation.fields, trieKey, dataFileSize, rowCount,
                trieMetadataCalculator.build(), hllCalculator.build()
            )
        }
    }

    override fun close() {
        liveRelation.close()
    }
}
