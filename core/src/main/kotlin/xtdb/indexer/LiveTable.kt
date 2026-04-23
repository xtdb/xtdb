package xtdb.indexer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.*
import xtdb.arrow.VectorType.Mono
import xtdb.log.proto.TrieMetadata
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.*
import xtdb.util.HLL
import xtdb.util.RowCounter

class LiveTable @JvmOverloads constructor(
    private val al: BufferAllocator,
    val table: TableRef,
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

    private val trieMetadataCalculator = TrieMetadataCalculator(
        iidVec, validFromVec, validToVec, systemFromVec
    )

    private val hllCalculator = HllCalculator()

    fun importData(data: RelationReader) {
        val offset = liveRelation.rowCount
        val count = data.rowCount
        liveRelation.append(data)
        liveTrie = liveTrie.addRange(offset, count)
        trieMetadataCalculator.update(offset, offset + count)
        hllCalculator.update(opVec, offset, offset + count)
        rowCounter.addRows(count)
    }

  data class BlockMetadata(
        val vecTypes: Map<FieldName, VectorType>,
        val rowCount: Int,
        val hllDeltas: Map<FieldName, HLL>
    )

    fun blockMetadata(): BlockMetadata? {
        val rowCount = liveRelation.rowCount
        if (rowCount == 0) return null
        return BlockMetadata(
            vecTypes = liveRelation.logRelTypes.orEmpty(),
            rowCount = rowCount,
            hllDeltas = hllCalculator.build()
        )
    }

    data class FinishedBlock(
        val vecTypes: Map<FieldName, VectorType>,
        val trieKey: TrieKey,
        val dataFileSize: FileSize,
        val rowCount: Int,
        val trieMetadata: TrieMetadata,
        val hllDeltas: Map<FieldName, HLL>
    )

    fun finishBlock(bp: BufferPool, blockIdx: BlockIndex): FinishedBlock? {
        val rowCount = liveRelation.rowCount
        if (rowCount == 0) return null
        val trieKey = Trie.l0Key(blockIdx).toString()

        return liveRelation.openDirectSlice(al).use { dataRel ->
            val trieWriter = LiveTrieWriter(al, bp, calculateBlooms = false)
            val dataFileSize = trieWriter.writeLiveTrie(table, trieKey, liveTrie, dataRel)
            FinishedBlock(
                vecTypes = liveRelation.logRelTypes.orEmpty(),
                trieKey = trieKey,
                dataFileSize = dataFileSize,
                rowCount = rowCount,
                trieMetadata = trieMetadataCalculator.build(),
                hllDeltas = hllCalculator.build()
            )
        }
    }

    companion object {
        internal val RelationReader.logRelTypes: Map<String, VectorType>?
            get() {
                val putVec = vectorFor("op").vectorForOrNull("put") ?: return null
                val type = putVec.type
                check(type is Mono && type.arrowType == STRUCT_TYPE) {
                    "Expected 'put' vector to be STRUCT type, got: $type"
                }
                return type.children
            }

        @JvmStatic
        fun Map<TableRef, LiveTable>.finishBlock(bp: BufferPool, blockIdx: BlockIndex): Map<TableRef, FinishedBlock> =
            // migrated here because of #5107 - may migrate the rest later.
            runBlocking {
                this@finishBlock
                    .map { (tableName, liveTable) ->
                        async(Dispatchers.IO) {
                            tableName to liveTable.finishBlock(bp, blockIdx)
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
