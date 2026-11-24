package xtdb.index

import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.Bytes
import xtdb.api.error.Fault
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.indexer.Snapshot
import xtdb.storage.BufferPool
import xtdb.table.IndexFiles
import xtdb.trie.ArrowHashTrie
import xtdb.trie.ColumnName
import xtdb.trie.Trie
import xtdb.util.toByteArray
import java.util.SortedSet
import java.util.TreeSet

/** What an equality probe of an index found: the candidate IIDs, and the index tries and data pages it read to find them. */
class IndexProbe(val iids: SortedSet<ByteArray>, val triesRead: Long, val pagesRead: Long)

object IndexProber {
    /**
     * The candidate IIDs for a value hashing to any of [hashes] in [index]'s column, as of [snapshot]:
     * the IIDs [index]'s Live tries hold for them, with every live-tier row whose column hashes to one of them.
     *
     * [index] is Ready in [snapshot], so its Live tries cover every block the snapshot's trie catalog does, and the live tier the rest.
     */
    suspend fun probe(al: BufferAllocator, bp: BufferPool, snapshot: Snapshot, index: SecondaryIndex, hashes: Set<Long>): IndexProbe {
        val table = index.def.table
        val files = IndexFiles(snapshot.slug(table), index.def.id)
        val iids = TreeSet(Bytes.COMPARATOR)
        var pagesRead = 0L

        val tries = index.liveTries.filter { trie -> hashes.any { trie.key.partHolds(it) } }
        for (trie in tries) {
            val dataPath = files.dataFilePath(trie.key.toString())

            for (pageIdx in leafPageIdxs(al, bp, files, trie.key, hashes.filter { trie.key.partHolds(it) })) {
                pagesRead++
                Relation(al, bp.getFooter(dataPath).schema).use { rel ->
                    bp.getRecordBatch(dataPath, pageIdx).use { rel.load(it) }
                    val hashRdr = rel["_hash"]
                    val iidRdr = rel["_iid"]
                    for (idx in 0 until rel.rowCount)
                        if (hashRdr.getLong(idx) in hashes) iids += iidRdr.getBytes(idx).toByteArray()
                }
            }
        }

        for (live in snapshot.table(table)) iids.addLiveMatches(live.relation, index.def.column, hashes)

        return IndexProbe(iids, tries.size.toLong(), pagesRead)
    }

    @JvmStatic
    fun probeSync(al: BufferAllocator, bp: BufferPool, snapshot: Snapshot, index: SecondaryIndex, hashes: Set<Long>) =
        runBlocking { probe(al, bp, snapshot, index, hashes) }

    // Every entry for a hash is in the one leaf its bits lead to.
    private suspend fun leafPageIdxs(al: BufferAllocator, bp: BufferPool, files: IndexFiles, key: Trie.Key, hashes: List<Long>): SortedSet<Int> {
        val metaPath = files.metaFilePath(key.toString())

        return Relation(al, bp.getFooter(metaPath).schema).use { metaRel ->
            bp.getRecordBatch(metaPath, 0).use { metaRel.load(it) }
            ArrowHashTrie(metaRel["nodes"]).use { trie ->
                hashes.mapNotNullTo(TreeSet()) { leafPageIdx(key, trie.rootNode, it, key.part?.size() ?: 0) }
            }
        }
    }

    private tailrec fun leafPageIdx(key: Trie.Key, node: ArrowHashTrie.Node?, hash: Long, depth: Int): Int? =
        when (node) {
            null -> null
            is ArrowHashTrie.Leaf -> node.dataPageIndex
            is ArrowHashTrie.IidBranch -> leafPageIdx(key, node.hashChildren[indexBucket(hash, depth)], hash, depth + 1)
            is ArrowHashTrie.RecencyBranch ->
                throw Fault(
                    "index trie $key has a recency branch", "xtdb.index/recency-branch-in-index-trie",
                    mapOf("index-trie-key" to key.toString()),
                )
        }

    private fun MutableSet<ByteArray>.addLiveMatches(rel: RelationReader, column: ColumnName, hashes: Set<Long>) {
        val colRdr = rel["op"].vectorForOrNull("put")?.vectorForOrNull(column) ?: return
        val iidRdr = rel["_iid"]

        for (idx in 0 until rel.rowCount)
            if (!colRdr.isNull(idx) && IndexHash.of(colRdr, idx) in hashes) this += iidRdr.getBytes(idx).toByteArray()
    }
}
