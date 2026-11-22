package xtdb.index

import org.apache.arrow.memory.BufferAllocator
import xtdb.Bytes
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.IID
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.schema
import xtdb.storage.BufferPool
import xtdb.table.IndexFiles
import xtdb.table.TrieFiles
import xtdb.trie.BlockIndex
import xtdb.trie.ColumnName
import xtdb.trie.DataFileWriter
import xtdb.trie.FileSize
import xtdb.trie.MetadataFileWriter
import xtdb.trie.Trie
import xtdb.trie.TrieKey
import java.lang.Long.compareUnsigned
import java.nio.ByteBuffer
import java.util.PriorityQueue
import java.util.TreeSet

private const val BUCKET_BITS = 2
private const val BUCKETS = 1 shl BUCKET_BITS

private const val MAX_DEPTH = Long.SIZE_BITS / BUCKET_BITS - 1

/** The bucket [hash] falls in at [depth] of an index trie: the hash's bits from the top, two per level. */
fun indexBucket(hash: Long, depth: Int): Int =
    ((hash ushr (Long.SIZE_BITS - BUCKET_BITS * (depth + 1))) and (BUCKETS - 1).toLong()).toInt()

/** Whether [hash] falls in this index trie key's part: the buckets [hash] takes from the root down to the trie's own depth. */
fun Trie.Key.partHolds(hash: Long): Boolean =
    (part?.toArray() ?: ByteArray(0)).withIndex().all { (depth, bucket) -> indexBucket(hash, depth) == bucket.toInt() }

/**
 * Writes an index's tries: files of `(_hash, _iid)` rows, in unsigned-hash-then-IID order with no pair repeated,
 * under a hash trie whose branches bucket on the hash's bits from the top.
 *
 * The meta file takes a data trie's schema with no recency branches and no column metadata in its leaves.
 */
class IndexTrieWriter @JvmOverloads constructor(
    private val al: BufferAllocator, private val bp: BufferPool,
    private val pageLimit: Int = 1024,
) {
    private class Entry(val hash: Long, val iid: ByteArray)

    private val entryOrder = Comparator<Entry> { a, b ->
        compareUnsigned(a.hash, b.hash).takeIf { it != 0 } ?: Bytes.COMPARATOR.compare(a.iid, b.iid)
    }

    /**
     * Writes the L0 for block [blockIdx] of an index on [column], from [rel]'s put rows — null where none of them gives the column a value.
     *
     * [rel] is in the live index's layout: an `_iid` column and an `op` union whose `put` leg holds the document.
     */
    suspend fun writeL0(files: TrieFiles, blockIdx: BlockIndex, rel: RelationReader, column: ColumnName): IndexTrie? =
        writeL0(files, blockIdx, entries().also { it.addPuts(rel, column) })

    /**
     * Writes the L0 for block [blockIdx] of an index on [column], from the put rows of the table's data L0 for that block —
     * null where the table has no data L0 for the block, or none of its put rows gives the column a value.
     */
    suspend fun backfillL0(files: IndexFiles, blockIdx: BlockIndex, column: ColumnName): IndexTrie? {
        val table = files.table
        val dataPath = table.dataFilePath(Trie.l0Key(blockIdx).toString())
        if (bp.listAllObjects(table.dataFileDir()).none { it.key == dataPath }) return null

        val entries = entries()
        val footer = bp.getFooter(dataPath)
        Relation(al, footer.schema).use { rel ->
            for (pageIdx in footer.recordBatches.indices) {
                bp.getRecordBatch(dataPath, pageIdx).use { rel.load(it) }
                entries.addPuts(rel, column)
            }
        }

        return writeL0(files, blockIdx, entries)
    }

    /**
     * Writes an index compaction's [outputs] from the set union of its [inputs]' entries.
     *
     * Each output takes the entries whose hash its part prefixes, as [indexBucket] reads a hash, so an output with no
     * part takes them all; an output no entry falls in isn't written.
     */
    suspend fun compact(files: TrieFiles, inputs: List<Trie.Key>, outputs: List<Trie.Key>): List<IndexTrie> {
        val outputEntries = outputs.associateWith { mutableListOf<Entry>() }

        union(files, inputs) { entry ->
            outputEntries.entries.firstOrNull { (key, _) -> key.partHolds(entry.hash) }?.value?.add(entry)
        }

        return outputEntries.mapNotNull { (key, entries) ->
            if (entries.isEmpty()) null
            else IndexTrie(key, write(files, key.toString(), entries, key.part?.size() ?: 0))
        }
    }

    private inner class Cursor(files: TrieFiles, key: Trie.Key) {
        private val path = files.dataFilePath(key.toString())
        private var pageCount = -1
        private var nextPage = 0
        private var page = emptyList<Entry>()
        private var idx = 0

        val entry: Entry get() = page[idx]

        suspend fun advance(): Boolean {
            if (++idx < page.size) return true

            val footer = bp.getFooter(path)
            if (pageCount < 0) pageCount = footer.recordBatches.size

            while (nextPage < pageCount) {
                page = Relation(al, footer.schema).use { rel ->
                    bp.getRecordBatch(path, nextPage++).use { rel.load(it) }
                    val hashes = rel["_hash"]
                    val iids = rel["_iid"]
                    List(rel.rowCount) { Entry(hashes.getLong(it), iids.getBytes(it).toBytes()) }
                }
                idx = 0
                if (page.isNotEmpty()) return true
            }

            return false
        }
    }

    private suspend fun union(files: TrieFiles, inputs: List<Trie.Key>, f: (Entry) -> Unit) {
        val heap = PriorityQueue<Cursor>(maxOf(inputs.size, 1)) { a, b -> entryOrder.compare(a.entry, b.entry) }
        for (key in inputs) Cursor(files, key).let { if (it.advance()) heap += it }

        var last: Entry? = null
        while (heap.isNotEmpty()) {
            val cursor = heap.poll()
            val entry = cursor.entry
            if (last == null || entryOrder.compare(last, entry) != 0) {
                f(entry)
                last = entry
            }
            if (cursor.advance()) heap += cursor
        }
    }

    private fun entries() = TreeSet(entryOrder)

    private fun MutableSet<Entry>.addPuts(rel: RelationReader, column: ColumnName) {
        val colRdr = rel["op"].vectorForOrNull("put")?.vectorForOrNull(column) ?: return
        val iidRdr = rel["_iid"]

        for (idx in 0 until rel.rowCount)
            if (!colRdr.isNull(idx))
                this += Entry(IndexHash.of(colRdr, idx), iidRdr.getBytes(idx).toBytes())
    }

    private suspend fun writeL0(files: TrieFiles, blockIdx: BlockIndex, entries: Set<Entry>): IndexTrie? {
        if (entries.isEmpty()) return null

        val key = Trie.l0Key(blockIdx)
        return IndexTrie(key, write(files, key.toString(), entries.toList(), 0))
    }

    private fun ByteBuffer.toBytes() = ByteArray(remaining()).also { duplicate().get(it) }

    // [startDepth] is the depth of the trie's root in the index's tree: the length of its key's part.
    private suspend fun write(files: TrieFiles, trieKey: TrieKey, entries: List<Entry>, startDepth: Int): FileSize =
        Relation(al, MetadataFileWriter.metaRelSchema(recencyBranches = false)).use { metaRel ->
            val nodes = metaRel["nodes"]
            val branch = nodes["branch-iid"]
            val branchEls = branch.listElements
            val leaf = nodes["leaf"]
            val leafPageIdx = leaf["data-page-idx"]
            val leafCols = leaf["columns"]

            DataFileWriter(al, bp, files, trieKey, schema("_hash" ofType I64, "_iid" ofType IID)).use { dataWriter ->
                val dataRel = dataWriter.dataRel
                val hashWtr = dataRel["_hash"]
                val iidWtr = dataRel["_iid"]
                var pageCount = 0

                fun writeNode(from: Int, to: Int, depth: Int): Int {
                    if (to - from <= pageLimit || depth > MAX_DEPTH) {
                        for (entry in entries.subList(from, to)) {
                            hashWtr.writeLong(entry.hash)
                            iidWtr.writeBytes(entry.iid)
                            dataRel.endRow()
                        }
                        dataWriter.writePage()

                        val nodeIdx = nodes.valueCount
                        leafCols.endList()
                        leafPageIdx.writeInt(pageCount++)
                        leaf.endStruct()
                        metaRel.endRow()
                        return nodeIdx
                    }

                    val children = IntArray(BUCKETS) { -1 }
                    var start = from
                    while (start < to) {
                        val bucket = indexBucket(entries[start].hash, depth)
                        var end = start
                        while (end < to && indexBucket(entries[end].hash, depth) == bucket) end++
                        children[bucket] = writeNode(start, end, depth + 1)
                        start = end
                    }

                    val nodeIdx = nodes.valueCount
                    for (child in children) if (child < 0) branchEls.writeNull() else branchEls.writeInt(child)
                    branch.endList()
                    metaRel.endRow()
                    return nodeIdx
                }

                writeNode(0, entries.size, startDepth)

                val dataFileSize = dataWriter.end()

                bp.openArrowWriter(files.metaFilePath(trieKey), metaRel).use { metaWriter ->
                    metaWriter.writePage()
                    metaWriter.end()
                }

                dataFileSize
            }
        }
}
