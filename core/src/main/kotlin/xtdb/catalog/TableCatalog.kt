package xtdb.catalog

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.flow.updateAndGet
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.api.TableRef
import xtdb.api.TransactionKey
import xtdb.api.log.LeaderTerm
import xtdb.api.storage.ObjectStore
import xtdb.api.tx.BlockDetails
import xtdb.api.tx.ExternalSourceToken
import xtdb.arrow.MergeTypes.Companion.joinContributions
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.asType
import xtdb.arrow.VectorType.Companion.field
import xtdb.block.proto.Block
import xtdb.block.proto.Partition
import xtdb.block.proto.TableBlock
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.database.proto.DatabaseConfig
import xtdb.indexer.LiveTable
import xtdb.storage.BufferPool
import xtdb.table.TableEntry
import xtdb.table.TableSlug
import xtdb.table.fromSchemaAndTable
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.BlockIndex
import xtdb.trie.ColumnName
import xtdb.types.MessageId
import xtdb.util.HLL
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import xtdb.util.combine
import xtdb.util.deserializeMessageAsSchemaInterruptibly
import xtdb.util.serializeAsMessageInterruptibly
import xtdb.util.toHLL
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.io.path.extension

/**
 * A block written before the registry existed carries names alone, and every table in it is on disk under
 * its escaped name — so that is the slug each takes, and their files stay where they are.
 *
 * Oids come from the sorted names rather than from list order: the proto's order comes from a Set, so it
 * isn't stable, and every node has to derive the same registry from the same bytes.
 */
private fun Block.tableEntries(): List<TableEntry> =
    tablesList.takeIf { it.isNotEmpty() }?.map { TableEntry.fromProto(it) }
        ?: tableNamesList.sorted().mapIndexed { idx, name -> TableEntry.mint(idx + 1L, fromSchemaAndTable(name)) }

// The proto is the storage format, so this is the only place that reads its presence flags: everything
// downstream sees ordinary Kotlin nullability.
private fun Block.asBlockDetails() = BlockDetails(
    blockIndex = blockIndex,
    latestCompletedTx = takeIf { it.hasLatestCompletedTx() }?.latestCompletedTx
        ?.let { TransactionKey(it.txId, it.systemTime.microsAsInstant) },
    latestProcessedMsgId = latestProcessedMsgId.takeIf { hasLatestProcessedMsgId() },
    boundaryReplicaMsgId = boundaryReplicaMsgId.takeIf { hasBoundaryReplicaMsgId() },
    termId = termId,
    externalSourceToken = takeIf { it.hasExternalSourceToken() }?.externalSourceToken?.toByteArray(),
    tables = tableEntries(),
    secondaryDatabases = secondaryDatabasesMap,
)

private fun <T, R> StateFlow<T>.mapState(f: (T) -> R): StateFlow<R> = object : StateFlow<R> {
    override val value get() = f(this@mapState.value)
    override val replayCache get() = listOf(value)

    override suspend fun collect(collector: FlowCollector<R>): Nothing {
        this@mapState.map(f).distinctUntilChanged().collect(collector)
        error("a StateFlow never completes")
    }
}

/** What one database partition knows about its tables, and the block that knowledge is current to. */
class TableCatalog(private val bufferPool: BufferPool, initialBlock: Block? = null) {

    internal data class TableMeta(
        val vecTypes: Map<ColumnName, VectorType>,
        val rowCount: Long,
        val hlls: Map<ColumnName, HLL>
    ) {
        /** @see xtdb.indexer.TableSnapshot.contributedType — the historical side of the same memo. */
        val absentContribution by lazy { VectorType.absentContribution(vecTypes) }
    }

    /**
     * One version of the catalog, and the unit every write replaces wholesale.
     *
     * Reading through a [State] rather than through the accessors below is what stops two reads a query
     * apart seeing two versions — the value is immutable, so a later write cannot reach one already handed
     * out.
     */
    data class State internal constructor(
        val block: BlockDetails?,
        internal val tables: Map<TableRef, TableMeta>,
    ) {
        val blockIdx: BlockIndex? get() = block?.blockIndex

        // Copies the outer map on each read - the per-table maps are shared references, not copies. Cheap
        // where it is used: `buildTableInfo` reads it once per `Snapshot.open`. Worth knowing before
        // reaching for it per column.
        val types: Map<TableRef, Map<ColumnName, VectorType>>
            get() = tables.mapValues { (_, meta) -> meta.vecTypes }

        fun rowCount(table: TableRef): Long? = tables[table]?.rowCount

        /** The historical half's contribution — an unknown table has written nothing. */
        fun contributedType(table: TableRef, col: ColumnName): VectorType =
            tables[table]?.let { it.vecTypes[col] ?: it.absentContribution } ?: VectorType.Nothing

        val entries: List<TableEntry> get() = block?.tables.orEmpty()

        private val entriesByTable by lazy { entries.associateBy { it.table } }

        fun entry(table: TableRef): TableEntry? = entriesByTable[table]

        /**
         * Where [table]'s files live.
         *
         * Resolving through a pinned [State] is what keeps a query's paths consistent with the trie state it
         * planned against: the block it read is the block whose slugs it uses.
         *
         * A table the registry doesn't hold has yet to reach a block, so nothing of it is on disk under any
         * other name and the derived slug is the one its first write will record.
         */
        fun slug(table: TableRef): TableSlug = entry(table)?.slug ?: TableSlug.of(table)
    }

    private val _state = MutableStateFlow(State(initialBlock?.asBlockDetails(), emptyMap()))

    fun snap(): State = _state.value

    /**
     * The latest block this catalog knows to be in object storage, advancing on [refresh] — which the
     * leader calls once the block file has landed, and a follower once it has read the block back.
     *
     * A collector is therefore observing durability, not resolution: anything this emits is recoverable
     * from storage alone. That is what lets an external source use the emitted `externalSourceToken` as
     * the furthest position it may confirm upstream.
     */
    val latestBlock: StateFlow<BlockDetails?> = _state.mapState { it.block }

    val currentBlockIndex: BlockIndex? get() = snap().blockIdx

    val latestCompletedTx: TransactionKey? get() = snap().block?.latestCompletedTx

    val latestProcessedMsgId: MessageId?
        get() = snap().block.let { it?.latestProcessedMsgId ?: it?.latestCompletedTx?.txId }

    val boundaryReplicaMsgId: MessageId? get() = snap().block?.boundaryReplicaMsgId

    // the leader term that produced the latest block's boundary; a follower seeds its read-side term
    // fence from here. Default 0 (plain scalar) for blocks written before term-fencing. See #5817.
    val boundaryTermId: Long get() = snap().block?.termId ?: LeaderTerm.NONE

    val externalSourceToken: ExternalSourceToken? get() = snap().block?.externalSourceToken

    val allTables: List<TableRef> get() = snap().entries.map { it.table }

    fun slug(table: TableRef): TableSlug = snap().slug(table)

    /**
     * The registry for a block covering [tables]: the entry this catalog already holds for each, carried
     * forward verbatim, and a freshly minted one for each table it doesn't.
     *
     * Carrying forward rather than recomputing is what freezes a slug for the table's lifetime. A leader
     * that re-derived them would move every path the moment the minting algorithm changed, orphaning
     * everything already written.
     *
     * Sorted, so that the same table set yields the same block bytes whichever node wrote it.
     */
    fun resolveTables(tables: Collection<TableRef>): List<TableEntry> {
        val existing = snap().entries.associateBy { it.table }
        val nextOid = (existing.values.maxOfOrNull { it.oid } ?: 0L) + 1

        val minted = tables.filterNot { it in existing }.sortedBy { it.schemaAndTable }
            .mapIndexed { idx, table -> TableEntry.mint(nextOid + idx, table) }
            .associateBy { it.table }

        return tables.sortedBy { it.schemaAndTable }.map { existing[it] ?: minted.getValue(it) }
    }

    val secondaryDatabases: Map<String, DatabaseConfig> get() = snap().block?.secondaryDatabases.orEmpty()

    fun rowCount(table: TableRef): Long? = snap().rowCount(table)

    /**
     * Seeds a data-backed system table (e.g. `xt.txs`) so it's present from startup, as if an empty
     * `CREATE TABLE` had run: columns are declared but dataless (`Nothing`), so the first real write
     * sets their types (`Nothing ⊔ X = X`). No-op if the table was already loaded from storage.
     */
    fun seedTable(table: TableRef, colNames: List<ColumnName>) {
        val meta = TableMeta(colNames.associateWith { VectorType.Nothing }, 0, emptyMap())

        _state.update {
            if (it.tables.containsKey(table)) it else it.copy(tables = it.tables + (table to meta))
        }
    }

    /**
     * Loads table metadata from storage for the block this catalog opened at, replacing the in-memory
     * tables wholesale. Called once, at open, *before* [seedTable] - a second call would drop any seeds
     * not yet persisted to storage.
     */
    fun loadTables() {
        val snap = snap()
        val blockIndex = snap.blockIdx ?: return
        val tables = loadTablesFromStorage(bufferPool, snap.entries, blockIndex)

        _state.update { it.copy(tables = tables) }
    }

    /**
     * Advances to a newly durable block, folding that block's per-table metadata in with it.
     *
     * A re-delivered block leaves the block half alone but still folds the metadata.
     */
    fun refresh(block: Block?, metadata: Map<TableRef, LiveTable.BlockMetadata> = emptyMap()) {
        val delta = metadata.mapValues { (_, bm) -> TableMeta(bm.vecTypes, bm.rowCount.toLong(), bm.hllDeltas) }

        _state.update { cur ->
            State(
                block = if (block != null && block.blockIndex == cur.blockIdx) cur.block else block?.asBlockDetails(),
                tables = if (delta.isEmpty()) cur.tables else cur.tables.foldIn(delta)
            )
        }
    }

    fun finishBlock(
        tableMetadata: Map<TableRef, LiveTable.FinishedBlock>,
        tablePartitions: Map<TableRef, List<Partition>>
    ): Map<TableRef, TableBlock> {
        val delta = tableMetadata.mapValues { (_, fb) -> TableMeta(fb.vecTypes, fb.rowCount.toLong(), fb.hllDeltas) }

        return _state.updateAndGet { it.copy(tables = it.tables.foldIn(delta)) }
            .tables
            .mapValues { (table, meta) ->
                buildTableBlock(meta.vecTypes, meta.rowCount, tablePartitions[table].orEmpty(), meta.hlls)
            }
    }

    fun buildBlock(
        blockIndex: BlockIndex,
        latestCompletedTx: TransactionKey?,
        latestProcessedMsgId: MessageId,
        boundaryReplicaMsgId: MessageId?,
        tables: Collection<TableEntry>,
        secondaryDatabases: Map<String, DatabaseConfig>?,
        externalSourceToken: ExternalSourceToken? = null,
        termId: Long = LeaderTerm.NONE,
    ): Block {
        val currentBlockIndex = this.currentBlockIndex
        check(currentBlockIndex == null || currentBlockIndex < blockIndex) {
            "Cannot finish block $blockIndex when current block is $currentBlockIndex"
        }

        return block {
            this.blockIndex = blockIndex
            latestCompletedTx?.also { tx ->
                this.latestCompletedTx = txKey {
                    txId = tx.txId
                    systemTime = tx.systemTime.asMicros
                }
            }
            this.latestProcessedMsgId = latestProcessedMsgId
            boundaryReplicaMsgId?.let { this.boundaryReplicaMsgId = it }
            // table_names is how a reader predating the registry finds the tables, so it keeps being
            // written. See #4037.
            this.tableNames.addAll(tables.map { it.table.sym.toString() })
            this.tables.addAll(tables.map { it.toProto() })
            secondaryDatabases?.let { this.secondaryDatabases.putAll(it) }
            externalSourceToken?.let { this.externalSourceToken = ByteString.copyFrom(it) }
            this.termId = termId
        }
    }

    companion object {
        private val blocksPath = "blocks".asPath

        @JvmStatic
        fun blockFilePath(blockIndex: BlockIndex): Path =
            blocksPath.resolve("b${blockIndex.asLexHex}.binpb")

        @JvmStatic
        fun tableBlockPath(table: TableSlug, blockIndex: BlockIndex): Path =
            table.tablePath.resolve(blocksPath).resolve("b${blockIndex.asLexHex}.binpb")

        val BufferPool.allBlockFiles: Iterable<ObjectStore.StoredObject>
            get() = listAllObjects(blocksPath).filter { it.key.fileName.extension == "binpb" }

        fun BufferPool.tableBlocks(table: TableSlug): Iterable<ObjectStore.StoredObject> =
            listAllObjects(table.tablePath.resolve(blocksPath))

        @JvmStatic
        val BufferPool.latestBlock: Block?
            get() = allBlockFiles.lastOrNull()?.key
                ?.let { blockKey -> Block.parseFrom(getByteArray(blockKey)) }

        fun BufferPool.blockFromLatest(distance: Int): Block? =
            allBlockFiles.toList().dropLast(maxOf(0, distance - 1)).lastOrNull()?.key
                ?.let { blockKey -> Block.parseFrom(getByteArray(blockKey)) }

        private fun parseTableBlock(tableBlock: TableBlock) =
            TableMeta(
                tableBlock.arrowSchema.toByteArray()
                    .let { ByteBuffer.wrap(it).deserializeMessageAsSchemaInterruptibly() }
                    .fields.associate { field -> field.name to field.asType },
                tableBlock.rowCount,
                tableBlock.columnNameToHllMap.mapValues { (_, bs) -> toHLL(bs.toByteArray()) }
            )

        internal fun loadTablesFromStorage(
            bufferPool: BufferPool, entries: List<TableEntry>, blockIndex: BlockIndex
        ): Map<TableRef, TableMeta> =
            entries.associate { entry ->
                entry.table to
                        parseTableBlock(
                            TableBlock.parseFrom(bufferPool.getByteArray(tableBlockPath(entry.slug, blockIndex)))
                        )
            }

        /** Folds a block's per-table deltas into the accumulated catalog. */
        private fun Map<TableRef, TableMeta>.foldIn(delta: Map<TableRef, TableMeta>) =
            (keys + delta.keys).associateWith { mergeTables(this[it], delta[it]) }

        /**
         * Folds a block's types into the accumulated catalog.
         *
         * Each side is a partial map read as a total function - its recorded type where it has one, and its
         * [VectorType.absentContribution] everywhere else. Substituting `Null` unconditionally would be right
         * only for a side that holds put rows; for one with none (delete-only, or a `CREATE TABLE` declaring
         * a column and writing nothing) it widens every column that side failed to mention, and since types
         * only ever widen and this result is serialised into the block file, that would never heal (#5911).
         */
        internal fun mergeVecTypes(
            old: Map<ColumnName, VectorType>?, new: Map<ColumnName, VectorType>?
        ): Map<ColumnName, VectorType> =
            when {
                old == null -> new.orEmpty()
                new == null -> old
                else -> {
                    val oldAbsent = VectorType.absentContribution(old)
                    val newAbsent = VectorType.absentContribution(new)

                    (old.keys + new.keys).associateWith { col ->
                        joinContributions(listOf(old[col] ?: oldAbsent, new[col] ?: newAbsent))
                    }
                }
            }

        internal fun mergeHlls(old: Map<ColumnName, HLL>?, new: Map<ColumnName, HLL>?): Map<ColumnName, HLL> =
            when {
                old == null -> new.orEmpty()
                new == null -> old
                else -> {
                    val allCols = old.keys + new.keys
                    allCols.associateWith { col ->
                        val o = old[col]
                        val n = new[col]
                        when {
                            o == null -> n!!
                            n == null -> o
                            else -> o.combine(n)
                        }
                    }
                }
            }

        internal fun mergeTables(old: TableMeta?, delta: TableMeta?): TableMeta {
            if (old == null && delta == null) return TableMeta(emptyMap(), 0, emptyMap())
            if (old == null) return delta!!
            if (delta == null) return old
            return TableMeta(
                vecTypes = mergeVecTypes(old.vecTypes, delta.vecTypes),
                rowCount = old.rowCount + delta.rowCount,
                hlls = mergeHlls(old.hlls, delta.hlls)
            )
        }

        internal fun buildTableBlock(
            vecTypes: Map<ColumnName, VectorType>,
            rowCount: Long,
            partitions: List<Partition>,
            hlls: Map<ColumnName, HLL>
        ): TableBlock {
            val schema = Schema(vecTypes.map { (colName, vecType) -> field(colName, vecType) })

            return TableBlock.newBuilder()
                .apply {
                    this.arrowSchema = ByteString.copyFrom(schema.serializeAsMessageInterruptibly())
                    this.rowCount = rowCount
                    putAllColumnNameToHll(hlls.mapValues { (_, hll) -> ByteString.copyFrom(hll.duplicate()) })
                    addAllPartitions(partitions)
                }
                .build()
        }
    }
}
