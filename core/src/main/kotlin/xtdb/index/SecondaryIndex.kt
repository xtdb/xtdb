package xtdb.index

import xtdb.api.TableRef
import xtdb.api.error.Fault
import xtdb.block.proto.SecondaryIndexEntry
import xtdb.block.proto.TableIndices
import xtdb.block.proto.buildingIndex
import xtdb.block.proto.droppedIndex
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
    /** Declared, and not yet usable by a scan. */
    data object Building : IndexState

    /**
     * Dropped by the transaction at [asOf]: out of the name set, and never used by a scan from then on.
     * The index stays in the catalog until its files are collected.
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

/**
 * [firstFlushedBlock] is the block that was open when the declaration applied: its flush, and every later one,
 * covers this index, and the blocks below it are the ones a backfill has to cover.
 */
data class SecondaryIndex(val def: IndexDef, val firstFlushedBlock: BlockIndex, val state: IndexState, val tries: List<IndexTrie>) {
    fun toProto(): SecondaryIndexEntry = secondaryIndexEntry {
        definition = def.toProto()
        firstFlushedBlock = this@SecondaryIndex.firstFlushedBlock
        when (state) {
            IndexState.Building -> building = buildingIndex { }
            is IndexState.Dropped -> dropped = droppedIndex { asOfMicros = state.asOf.asMicros }
        }
        tries += this@SecondaryIndex.tries.map { it.toProto() }
    }

    companion object {
        fun fromProto(proto: SecondaryIndexEntry) =
            SecondaryIndex(
                IndexDef.fromProto(proto.definition),
                proto.firstFlushedBlock,
                when (proto.stateCase) {
                    SecondaryIndexEntry.StateCase.BUILDING -> IndexState.Building
                    SecondaryIndexEntry.StateCase.DROPPED -> IndexState.Dropped(fromMicros(proto.dropped.asOfMicros))
                    else -> throw Fault(
                        "secondary index ${proto.definition.indexId} has no state in its block",
                        "xtdb.index/missing-index-state",
                        mapOf("index-id" to proto.definition.indexId),
                    )
                },
                proto.triesList.map { IndexTrie.fromProto(it) },
            )
    }
}

/** One partition's secondary indices, as one value. */
class IndexCatalog private constructor(private val byId: Map<UUID, SecondaryIndex>) {

    val indices: Collection<SecondaryIndex> get() = byId.values

    /** Whether the catalog holds [id], in any state, dropped included. */
    operator fun contains(id: UUID): Boolean = id in byId

    /** The index declared under [name] and not since dropped — at most one. */
    fun declared(name: String): SecondaryIndex? =
        indices.firstOrNull { it.def.name == name && it.state !is IndexState.Dropped }

    /** [openBlock] is the block the live index is filling as the declaration applies. */
    fun withIndex(def: IndexDef, openBlock: BlockIndex): IndexCatalog {
        if (def.id in byId)
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

        return IndexCatalog(byId + (def.id to SecondaryIndex(def, openBlock, IndexState.Building, emptyList())))
    }

    fun dropped(id: UUID, asOf: Instant): IndexCatalog {
        val index = byId[id]
            ?: throw Fault("index $id is not in the catalog", "xtdb.index/unknown-index", mapOf("index-id" to id))

        if (index.state is IndexState.Dropped)
            throw Fault(
                "index ${index.def.name} ($id) is already dropped", "xtdb.index/index-already-dropped",
                mapOf("index-id" to id, "index-name" to index.def.name),
            )

        return IndexCatalog(byId + (id to index.copy(state = IndexState.Dropped(asOf))))
    }

    /** Adds a block's index L0s, [l0s] keyed by index id. */
    fun withL0s(l0s: Map<UUID, IndexTrie>): IndexCatalog =
        IndexCatalog(byId + l0s.map { (id, l0) ->
            val index = byId[id]
                ?: throw Fault("index $id is not in the catalog", "xtdb.index/unknown-index", mapOf("index-id" to id))

            id to index.copy(tries = index.tries + l0)
        })

    /** The tables an index here is declared on, dropped ones included: each one's block file records its indices. */
    val tables: Set<TableRef> get() = indices.mapTo(HashSet()) { it.def.table }

    /** [table]'s indices, as its block file records them. */
    fun toProto(table: TableRef): TableIndices = tableIndices {
        indices += this@IndexCatalog.indices.filter { it.def.table == table }.sortedBy { it.def.id.toString() }.map { it.toProto() }
    }

    companion object {
        @JvmField
        val EMPTY = IndexCatalog(emptyMap())

        /** The catalog the block files of a block's tables record, keyed by table. */
        @JvmStatic
        fun fromProto(tableIndices: Map<TableRef, TableIndices>) =
            IndexCatalog(tableIndices.values.flatMap { it.indicesList }.map { SecondaryIndex.fromProto(it) }.associateBy { it.def.id })
    }
}
