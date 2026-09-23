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
import xtdb.util.closeOnCatch

class LiveTable private constructor(
    private val al: BufferAllocator,
    val table: TableRef,
    val slug: TableSlug,
    val blockIdx: Long,
    private val rowCounter: RowCounter,
    private val liveTrieFactory: LiveTrieFactory,
    private val state: State,
) : AutoCloseable {

    /**
     * A live table's rows: the relation holding them, and the iid trie indexing them.
     *
     * This is what changes from one value of the table to the next — everything else about a live table
     * is fixed when it is opened, and the transient that produces the next value carries a [State] of
     * its own over the same relation.
     */
    class State(
        val relation: Relation,
        val trie: MemoryHashTrie,
    )

    @FunctionalInterface
    fun interface LiveTrieFactory {
        operator fun invoke(iidVec: VectorReader): MemoryHashTrie
    }

    val relation get() = state.relation
    val trie get() = state.trie

    private fun withState(state: State) =
        LiveTable(al, table, slug, blockIdx, rowCounter, liveTrieFactory, state)

    private val opVec = relation["op"]

    /**
     * One transaction's writes into a log-data relation, and the iid trie over them.
     *
     * Mutated in place by the coroutine that opened it, which is its only writer — it takes no lock, and
     * nothing may read it concurrently. [commit] yields the table's next value; closing it instead discards
     * the writes, and the next transaction overwrites them.
     */
    class Tx internal constructor(
        private val base: LiveTable,
        private val systemTimeMicros: Long,
        private var state: State,
    ) : AutoCloseable {

        val relation get() = state.relation

        /** Where this transaction's own rows start; the table already held everything below it. */
        val startRowIdx = relation.rowCount

        internal val iidVec = relation["_iid"]
        internal val validFromVec = relation["_valid_from"]
        internal val validToVec = relation["_valid_to"]
        internal val opVec = relation["op"]

        private val systemFromVec = relation["_system_from"]

        val trie get() = state.trie

        val rowCount get() = relation.rowCount

        /** Ends [count] rows whose columns are already written, stamping their system-from and indexing them. */
        fun endOps(count: Int) {
            val pos = relation.rowCount

            repeat(count) { systemFromVec.writeLong(systemTimeMicros) }

            relation.endRows(count)

            state = State(relation, state.trie.addRange(pos, count))
        }

        // Set once the value that takes this transient's relation exists, and only then: a constructor
        // that threw would leave the relation this transient's to free.
        private var committed = false

        /**
         * The table's next value, taking over this transient's rows.
         *
         * It takes the relation with them, so this transient is spent — [close] afterwards is a no-op,
         * the way a Clojure transient stops being usable once `persistent!` has taken it. Without that,
         * a caller closing both would release the relation twice while the value is still reading it.
         *
         * The rows reach [LiveIndex.blockRowCount] when that value is installed rather than here: that
         * count spans terms, where a transient is committed at resolve and a term ending before its
         * resolved transactions come back discards them.
         */
        fun commit(): LiveTable =
            base.withState(state).also { committed = true }

        override fun close() {
            if (!committed) relation.close()
        }
    }

    /**
     * This table's transient for one transaction — a writable slice over the same memory, starting at this
     * table's row count, whose appends this table cannot see.
     */
    fun openTx(systemTimeMicros: Long): Tx =
        relation.openDirectSlice(al).closeOnCatch { slice ->
            Tx(
                this, systemTimeMicros,
                State(slice, trie.withIidReader(slice["_iid"]))
            )
        }

    /**
     * This table's next value, with [data]'s rows appended.
     *
     * The relation is appended in place and so is shared with the value this is called on, which the
     * caller therefore replaces rather than keeping: what the next value holds of its own is the trie
     * over the wider range.
     */
    fun importData(data: RelationReader): LiveTable {
        val offset = relation.rowCount
        val count = data.rowCount

        relation.append(data)
        rowCounter.addRows(count)

        return withState(State(relation, trie.addRange(offset, count)))
    }

    data class BlockMetadata(
        val vecTypes: Map<FieldName, VectorType>,
        val rowCount: Int,
        val hllDeltas: Map<FieldName, HLL>
    )

    fun blockMetadata(): BlockMetadata {
        val rowCount = relation.rowCount
        return BlockMetadata(
            vecTypes = relation.logRelTypes.orEmpty(),
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
        val rowCount = relation.rowCount
        val vecTypes = relation.logRelTypes.orEmpty()
        val hllDeltas = computeHlls(opVec, 0, rowCount)

        if (rowCount == 0) return FinishedBlock(vecTypes, rowCount, hllDeltas, writtenTrie = null)

        val trieKey = Trie.l0Key(blockIdx).toString()

        return relation.openDirectSlice(al).use { dataRel ->
            val trieWriter = LiveTrieWriter(al, bp, calculateBlooms = false)
            val (dataFileSize, trieMetadata) = trieWriter.writeLiveTrie(slug, trieKey, trie, dataRel)
            FinishedBlock(
                vecTypes = vecTypes,
                rowCount = rowCount,
                hllDeltas = hllDeltas,
                writtenTrie = FinishedBlock.WrittenTrie(
                    trieKey = trieKey,
                    dataFileSize = dataFileSize,
                    trieMetadata = trieMetadata
                )
            )
        }
    }

    companion object {

        /**
         * A table with no rows yet, over a relation of its own.
         *
         * A factory rather than a constructor because the trie is built by reading a vector off that
         * relation, which can fail: a constructor delegation would have
         * nowhere to put the guard, and the relation would be orphaned with nothing holding it.
         */
        @JvmStatic
        @JvmOverloads
        fun open(
            al: BufferAllocator, table: TableRef, slug: TableSlug, blockIdx: Long, rowCounter: RowCounter,
            liveTrieFactory: LiveTrieFactory = LiveTrieFactory { MemoryHashTrie.emptyTrie(it) },
        ): LiveTable =
            Trie.openLogDataWriter(al).closeOnCatch { rel ->
                LiveTable(
                    al, table, slug, blockIdx, rowCounter, liveTrieFactory,
                    State(rel, liveTrieFactory(rel["_iid"]))
                )
            }

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
        relation.close()
    }
}
