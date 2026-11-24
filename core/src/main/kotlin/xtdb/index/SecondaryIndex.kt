package xtdb.index

import xtdb.api.TableRef
import xtdb.api.error.Fault
import xtdb.block.proto.SecondaryIndexEntry
import xtdb.block.proto.TableIndices
import xtdb.block.proto.buildingIndex
import xtdb.block.proto.droppedIndex
import xtdb.block.proto.garbageIndexTrie
import xtdb.block.proto.readyIndex
import xtdb.block.proto.secondaryIndexEntry
import xtdb.block.proto.tableIndices
import xtdb.log.proto.CreateIndex
import xtdb.log.proto.IndexTrieDetails
import xtdb.log.proto.createIndex
import xtdb.log.proto.indexTrieDetails
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import xtdb.trie.BlockIndex
import xtdb.trie.ColumnName
import xtdb.trie.FileSize
import xtdb.trie.Trie
import java.time.Instant
import java.util.UUID

/**
 * A declared index: what `CREATE INDEX` names.
 *
 * [id] is fixed when the declaration is submitted and travels the logs with it, so every node holds the same one,
 * and an index dropped and re-declared under the same [name] is a different index.
 */
data class IndexDef(val id: UUID, val name: String, val table: TableRef, val column: ColumnName) {
    fun toProto(): CreateIndex = createIndex {
        indexId = id.toString()
        indexName = name
        schemaName = table.schemaName
        tableName = table.tableName
        columnName = column
    }

    companion object {
        fun fromProto(proto: CreateIndex) =
            IndexDef(UUID.fromString(proto.indexId), proto.indexName, TableRef(proto.schemaName, proto.tableName), proto.columnName)
    }
}

sealed interface IndexState {
    /**
     * Declared, and not yet usable by a scan.
     *
     * [owedBlocks] are the blocks below the index's first flushed block that a backfill has still to cover,
     * whether or not the table wrote in them.
     * The index is complete once none are owed.
     */
    data class Building(val owedBlocks: Set<BlockIndex>) : IndexState {
        val complete: Boolean get() = owedBlocks.isEmpty()
    }

    /** Covers every block the node has adopted, so a scan may use it. */
    data object Ready : IndexState

    /**
     * Dropped by the transaction at [asOf]: out of the name set, and never used by a scan from then on.
     * Every trie it holds is Garbage, and it stays in the catalog until a GC cycle collects the last of them,
     * or, holding none, until the first cycle whose cutoff is past [asOf].
     */
    data class Dropped(val asOf: Instant) : IndexState
}

/** One file pair of an index's tree, at [key] in it. */
data class IndexTrie(val key: Trie.Key, val dataFileSize: FileSize) {
    fun toProto(): IndexTrieDetails = indexTrieDetails {
        trieKey = key.toString()
        dataFileSize = this@IndexTrie.dataFileSize
    }

    companion object {
        fun fromProto(proto: IndexTrieDetails) = IndexTrie(Trie.parseKey(proto.trieKey), proto.dataFileSize)
    }
}

sealed interface IndexTrieState {
    /** Read by a probe, and a candidate input to compaction. */
    data object Live : IndexTrieState

    /** Superseded, at [asOf], by tries holding every entry it holds; read only by a scan already holding it. */
    data class Garbage(val asOf: Instant) : IndexTrieState
}

/**
 * [firstFlushedBlock] is the block that was open when the declaration applied: its flush, and every later one,
 * covers this index, and the blocks below it are the ones a backfill has to cover.
 *
 * [tries] holds at most one trie per key.
 */
data class SecondaryIndex(
    val def: IndexDef, val firstFlushedBlock: BlockIndex, val state: IndexState, val tries: Map<IndexTrie, IndexTrieState>,
) {
    val liveTries: List<IndexTrie> get() = tries.filterValues { it == IndexTrieState.Live }.keys.toList()

    /** Whether GC deleting [keys] collects this index's last trie, so that it leaves the catalog. */
    fun isCollectedBy(keys: Collection<Trie.Key>): Boolean =
        state is IndexState.Dropped && tries.keys.all { it.key in keys }

    /** [this] with [added] Live, skipping any whose key it already holds, in whatever state. */
    fun withTries(added: Collection<IndexTrie>): SecondaryIndex {
        val held = tries.keys.mapTo(HashSet()) { it.key }
        return copy(tries = tries + added.filter { it.key !in held }.associateWith { IndexTrieState.Live })
    }

    /**
     * A compaction's [outputs], replacing [inputs] as of [asOf]: the outputs Live, the inputs Garbage.
     *
     * A no-op unless every input is still Live here, so a duplicate or late publish, whose inputs another job has
     * since replaced, can neither resurrect a superseded trie nor retire one twice.
     */
    fun compacted(outputs: Collection<IndexTrie>, inputs: Collection<Trie.Key>, asOf: Instant): SecondaryIndex {
        val live = liveTries.associateBy { it.key }
        if (!inputs.all { it in live }) return this

        val garbage = IndexTrieState.Garbage(asOf)
        return copy(tries = tries + inputs.associate { live.getValue(it) to garbage }).withTries(outputs)
    }

    fun toProto(): SecondaryIndexEntry = secondaryIndexEntry {
        definition = def.toProto()
        firstFlushedBlock = this@SecondaryIndex.firstFlushedBlock
        when (state) {
            is IndexState.Building -> building = buildingIndex { owedBlocks += state.owedBlocks.sorted() }
            IndexState.Ready -> ready = readyIndex { }
            is IndexState.Dropped -> dropped = droppedIndex { asOfMicros = state.asOf.asMicros }
        }

        for ((trie, trieState) in this@SecondaryIndex.tries.entries.sortedBy { it.key.key.toString() })
            when (trieState) {
                IndexTrieState.Live -> tries += trie.toProto()
                is IndexTrieState.Garbage -> garbageTries += garbageIndexTrie {
                    this.trie = trie.toProto()
                    garbageAsOfMicros = trieState.asOf.asMicros
                }
            }
    }

    companion object {
        fun fromProto(proto: SecondaryIndexEntry) =
            SecondaryIndex(
                IndexDef.fromProto(proto.definition),
                proto.firstFlushedBlock,
                when (proto.stateCase) {
                    SecondaryIndexEntry.StateCase.BUILDING -> IndexState.Building(proto.building.owedBlocksList.toSet())
                    SecondaryIndexEntry.StateCase.READY -> IndexState.Ready
                    SecondaryIndexEntry.StateCase.DROPPED -> IndexState.Dropped(fromMicros(proto.dropped.asOfMicros))
                    else -> throw Fault(
                        "secondary index ${proto.definition.indexId} has no state in its block",
                        "xtdb.index/missing-index-state",
                        mapOf("index-id" to proto.definition.indexId),
                    )
                },
                proto.triesList.associate { IndexTrie.fromProto(it) to IndexTrieState.Live } +
                        proto.garbageTriesList.associate {
                            IndexTrie.fromProto(it.trie) to IndexTrieState.Garbage(fromMicros(it.garbageAsOfMicros))
                        },
            )
    }
}

/**
 * One partition's secondary indices, as one value.
 *
 * [collected] holds the dropped indices whose every file GC has collected, each with the table it was on: out of [indices],
 * and never declarable again.
 */
class IndexCatalog private constructor(private val byId: Map<UUID, SecondaryIndex>, private val collected: Map<UUID, TableRef>) {

    val collectedIds: Set<UUID> get() = collected.keys

    val indices: Collection<SecondaryIndex> get() = byId.values

    /** Whether [id] was ever declared here, whatever became of its index since. */
    operator fun contains(id: UUID): Boolean = id in byId || id in collectedIds

    /** The index declared under [name] and not since dropped — at most one. */
    fun declared(name: String): SecondaryIndex? =
        indices.firstOrNull { it.def.name == name && it.state !is IndexState.Dropped }

    operator fun get(id: UUID): SecondaryIndex? = byId[id]

    /** A Ready index on [table]'s [column], if there is one. */
    fun readyOn(table: TableRef, column: ColumnName): SecondaryIndex? =
        indices.firstOrNull { it.state == IndexState.Ready && it.def.table == table && it.def.column == column }

    /**
     * [openBlock] is the block the live index is filling as the declaration applies;
     * [owedBlocks] are the blocks below it that a backfill has to cover before the index is complete.
     */
    fun withIndex(def: IndexDef, openBlock: BlockIndex, owedBlocks: Set<BlockIndex>): IndexCatalog {
        if (def.id in this)
            throw Fault(
                "index ${def.id} is already in the catalog", "xtdb.index/index-id-exists",
                mapOf("index-id" to def.id, "index-name" to def.name),
            )

        declared(def.name)?.let {
            throw Fault(
                "index '${def.name}' is already declared, as ${it.def.id}", "xtdb.index/index-name-declared",
                mapOf("index-name" to def.name, "index-id" to def.id, "declared-index-id" to it.def.id),
            )
        }

        return copy(byId + (def.id to SecondaryIndex(def, openBlock, IndexState.Building(owedBlocks), emptyMap())))
    }

    private fun copy(byId: Map<UUID, SecondaryIndex>, collected: Map<UUID, TableRef> = this.collected) = IndexCatalog(byId, collected)

    private fun unknownIndex(id: UUID) =
        Fault("index $id is not in the catalog", "xtdb.index/unknown-index", mapOf("index-id" to id))

    /** [id]'s index: null once it is collected, and an id never declared here faults. */
    private fun heldOrNull(id: UUID): SecondaryIndex? =
        byId[id] ?: if (id in collectedIds) null else throw unknownIndex(id)

    /** [f] of [id]'s index for a message about it: a no-op once it is collected, so a late message changes nothing. */
    private inline fun updating(id: UUID, f: (SecondaryIndex) -> IndexCatalog): IndexCatalog =
        heldOrNull(id)?.let(f) ?: this

    /** [index] with its tries replaced by [tries], leaving the catalog once it is dropped and holds none. */
    private fun withTriesOf(index: SecondaryIndex, tries: Map<IndexTrie, IndexTrieState>): IndexCatalog {
        val id = index.def.id
        return if (index.state is IndexState.Dropped && tries.isEmpty()) copy(byId - id, collected + (id to index.def.table))
        else copy(byId + (id to index.copy(tries = tries)))
    }

    /** Drops [id] as of [asOf]: every trie it holds becomes Garbage then. */
    fun dropped(id: UUID, asOf: Instant): IndexCatalog {
        val index = heldOrNull(id)
        if (index == null || index.state is IndexState.Dropped)
            throw Fault("index $id is already dropped", "xtdb.index/index-already-dropped", mapOf("index-id" to id))

        val garbage = IndexTrieState.Garbage(asOf)
        val tries = index.tries.mapValues { (_, state) -> state as? IndexTrieState.Garbage ?: garbage }
        return copy(byId + (id to index.copy(state = IndexState.Dropped(asOf), tries = tries)))
    }

    /**
     * The leader's decision that [id] is complete: Building becomes Ready.
     * A no-op on an index already Ready, or dropped since the decision, so re-applying it is harmless.
     */
    fun ready(id: UUID): IndexCatalog = updating(id) { index ->
        when (val state = index.state) {
            is IndexState.Building -> {
                if (!state.complete)
                    throw Fault(
                        "index ${index.def.name} ($id) is Ready on the log, but still owes blocks here",
                        "xtdb.index/index-not-complete",
                        mapOf("index-id" to id, "index-name" to index.def.name, "owed-blocks" to state.owedBlocks.sorted()),
                    )

                copy(byId + (id to index.copy(state = IndexState.Ready)))
            }

            IndexState.Ready, is IndexState.Dropped -> this
        }
    }

    /**
     * A backfill job's cover of [block] for [id]: adds its [l0], if it wrote one, and strikes [block] from the blocks owed.
     * A no-op unless [id] is Building and still owes [block], so a late or repeated cover, or one for an index since
     * dropped, changes nothing.
     */
    fun backfilled(id: UUID, block: BlockIndex, l0: IndexTrie?): IndexCatalog = updating(id) { index ->
        val state = index.state as? IndexState.Building
        if (state == null || block !in state.owedBlocks) this
        else copy(byId + (id to index.copy(state = state.copy(owedBlocks = state.owedBlocks - block)).withTries(listOfNotNull(l0))))
    }

    /** Adds a block's index L0s, [l0s] keyed by index id. */
    fun withL0s(l0s: Map<UUID, IndexTrie>): IndexCatalog =
        copy(byId + l0s.map { (id, l0) ->
            val index = byId[id] ?: throw unknownIndex(id)
            id to index.withTries(listOf(l0))
        })

    /** @see SecondaryIndex.compacted */
    fun compacted(id: UUID, outputs: Collection<IndexTrie>, inputs: Collection<Trie.Key>, asOf: Instant): IndexCatalog =
        updating(id) { index -> copy(byId + (id to index.compacted(outputs, inputs, asOf))) }

    /**
     * Each index's Garbage tries dated at or before [asOf], for those holding any,
     * and, with no keys, each dropped index holding no tries whose drop is at or before [asOf].
     */
    fun garbage(asOf: Instant): Map<SecondaryIndex, List<Trie.Key>> =
        indices.mapNotNull { index ->
            val keys = index.tries.mapNotNull { (trie, state) -> trie.key.takeIf { state is IndexTrieState.Garbage && !state.asOf.isAfter(asOf) } }
            val state = index.state
            val due = keys.isNotEmpty() || (index.tries.isEmpty() && state is IndexState.Dropped && !state.asOf.isAfter(asOf))
            (index to keys).takeIf { due }
        }.toMap()

    /**
     * GC's deletion of [keys] from [id]'s Garbage tries; a dropped index left holding none leaves the catalog.
     * A no-op on a key it holds Live or not at all.
     */
    fun withoutGarbage(id: UUID, keys: Set<Trie.Key>): IndexCatalog = updating(id) { index ->
        withTriesOf(index, index.tries.filterNot { (trie, state) -> state is IndexTrieState.Garbage && trie.key in keys })
    }

    /**
     * The tables an index here was ever declared on, through an index it holds or one it has collected:
     * each one's block file records its indices.
     */
    val tables: Set<TableRef> get() = indices.mapTo(HashSet()) { it.def.table } + collected.values

    /** [table]'s indices and collected ids, as its block file records them. */
    fun toProto(table: TableRef): TableIndices = tableIndices {
        indices += this@IndexCatalog.indices.filter { it.def.table == table }.sortedBy { it.def.id.toString() }.map { it.toProto() }
        collectedIds += collected.filterValues { it == table }.keys.map { it.toString() }.sorted()
    }

    companion object {
        @JvmField
        val EMPTY = IndexCatalog(emptyMap(), emptyMap())

        /** The catalog the block files of a block's tables record, keyed by table. */
        @JvmStatic
        fun fromProto(tableIndices: Map<TableRef, TableIndices>) =
            IndexCatalog(
                tableIndices.values.flatMap { it.indicesList }.map { SecondaryIndex.fromProto(it) }.associateBy { it.def.id },
                tableIndices.flatMap { (table, indices) -> indices.collectedIdsList.map { UUID.fromString(it) to table } }.toMap(),
            )
    }
}
