package xtdb.indexer

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.*
import xtdb.arrow.VectorType.Mono
import xtdb.log.proto.TrieMetadata
import xtdb.storage.BufferPool
import xtdb.table.TableSlug
import xtdb.api.TableRef
import xtdb.trie.*
import xtdb.util.HLL
import xtdb.util.RowCounter

class LiveTable @JvmOverloads constructor(
    private val al: BufferAllocator,
    val table: TableRef,
    val slug: TableSlug,
    val blockIdx: Long,
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
        validFromVec, validToVec, systemFromVec
    )

    fun importData(data: RelationReader) {
        val offset = liveRelation.rowCount
        val count = data.rowCount
        liveRelation.append(data)
        liveTrie = liveTrie.addRange(offset, count)
        trieMetadataCalculator.update(offset, offset + count)
        rowCounter.addRows(count)
    }

  data class BlockMetadata(
        val vecTypes: Map<FieldName, VectorType>,
        val rowCount: Int,
        val hllDeltas: Map<FieldName, HLL>
    )

    fun blockMetadata(): BlockMetadata {
        val rowCount = liveRelation.rowCount
        return BlockMetadata(
            vecTypes = liveRelation.logRelTypes.orEmpty(),
            rowCount = rowCount,
            hllDeltas = computeHlls(opVec, 0, rowCount)
        )
    }

    data class FinishedBlock(
        val vecTypes: Map<FieldName, VectorType>,
        val rowCount: Int,
        val hllDeltas: Map<FieldName, HLL>,
        /**
         * Null exactly when [rowCount] is zero.
         *
         * A table can be staged by a transaction without taking any rows from it — `CREATE TABLE`,
         * or DML whose predicate matched nothing — and it still has to reach the table catalog so
         * that its declared columns survive. It has no trie to write, though, and an empty L0 costs
         * two objects and a trie-catalog entry for nothing.
         */
        val writtenTrie: WrittenTrie?
    ) {
        data class WrittenTrie(
            val trieKey: TrieKey,
            val dataFileSize: FileSize,
            val trieMetadata: TrieMetadata
        )
    }

    /** For callers that aren't coroutine-native — see [finishBlock]. */
    fun finishBlockSync(bp: BufferPool, blockIdx: BlockIndex): FinishedBlock =
        runBlocking { finishBlock(bp, blockIdx) }

    suspend fun finishBlock(bp: BufferPool, blockIdx: BlockIndex): FinishedBlock {
        val rowCount = liveRelation.rowCount
        val vecTypes = liveRelation.logRelTypes.orEmpty()
        val hllDeltas = computeHlls(opVec, 0, rowCount)

        if (rowCount == 0) return FinishedBlock(vecTypes, rowCount, hllDeltas, writtenTrie = null)

        val trieKey = Trie.l0Key(blockIdx).toString()

        return liveRelation.openDirectSlice(al).use { dataRel ->
            val trieWriter = LiveTrieWriter(al, bp, calculateBlooms = false)
            val dataFileSize = trieWriter.writeLiveTrie(slug, trieKey, liveTrie, dataRel)
            FinishedBlock(
                vecTypes = vecTypes,
                rowCount = rowCount,
                hllDeltas = hllDeltas,
                writtenTrie = FinishedBlock.WrittenTrie(
                    trieKey = trieKey,
                    dataFileSize = dataFileSize,
                    trieMetadata = trieMetadataCalculator.build()
                )
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

        /**
         * Writes every table's block in parallel on [ioDispatcher].
         *
         * The dispatcher is a parameter rather than [Dispatchers.IO] because hardcoding it would put this
         * fan-out outside whatever dispatcher the caller runs on — which, under a deterministic simulation,
         * means outside the scheduler whose quiescence the simulation treats as its fixed point.
         */
        suspend fun Map<TableRef, LiveTable>.finishBlock(
            bp: BufferPool, blockIdx: BlockIndex, ioDispatcher: CoroutineDispatcher
        ): Map<TableRef, FinishedBlock> =
            coroutineScope {
                this@finishBlock
                    .map { (tableName, liveTable) ->
                        async(ioDispatcher) {
                            tableName to liveTable.finishBlock(bp, blockIdx)
                        }
                    }
                    .awaitAll()
                    .toMap()
            }
    }

    override fun close() {
        liveRelation.close()
    }
}
