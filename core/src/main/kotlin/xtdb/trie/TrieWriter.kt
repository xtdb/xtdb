package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.ArrowWriter
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.metadata.PageMetadataWriter
import xtdb.types.Fields
import xtdb.types.NamelessField.Companion.nullable
import xtdb.types.Schema
import xtdb.util.asPath
import xtdb.util.requiringResolve
import java.nio.file.Path
import java.util.*

class TrieWriter(
    allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    dataSchema: Schema,
    tableName: TableName,
    trieKey: TrieKey,
    private val writeContentMetadata: Boolean
) : AutoCloseable {
    companion object {
        @JvmStatic
        val tablesDir = "tables".asPath

        @JvmStatic
        val TableName.tablePath: Path get() = tablesDir.resolve(replace(Regex("[./]"), "\\$"))

        @JvmStatic
        fun dataFilePath(tableName: TableName, trieKey: TrieKey): Path =
            tableName.tablePath.resolve("data").resolve("$trieKey.arrow")

        @JvmStatic
        fun metaFilePath(tableName: TableName, trieKey: TrieKey): Path =
            tableName.tablePath.resolve("meta").resolve("$trieKey.arrow")

        private val metadataField = Fields.List(
            Fields.Struct(
                "col-name" to Fields.UTF8,
                "root-col?" to Fields.BOOL,
                "count" to Fields.I64,
                "types" to Fields.Struct(),
                "bloom" to Fields.VAR_BINARY.nullable
            ),
            elName = "struct"
        )

        @JvmStatic
        val metaRelSchema = Schema(
            "nodes" to Fields.Union(
                "nil" to Fields.NULL,
                "branch-iid" to Fields.List(nullable(Fields.I32)),
                "branch-recency" to Fields.Map(
                    "recency" to Fields.TEMPORAL,
                    "idx" to nullable(Fields.I32),
                ),
                "leaf" to Fields.Struct(
                    "data-page-idx" to Fields.I32,
                    "columns" to metadataField
                )
            )
        )

        @JvmStatic
        fun writeLiveTrie(
            al: BufferAllocator, bufferPool: BufferPool,
            tableName: TableName, trieKey: TrieKey,
            trie: MemoryHashTrie, dataRel: Relation
        ) =
            TrieWriter(al, bufferPool, dataRel.schema, tableName, trieKey, false)
                .use { writer ->
                    writer.writeLiveTrieNode(trie.compactLogs().rootNode, dataRel)
                    writer.end()
                }
    }

    val dataRel: Relation = Relation(allocator, dataSchema)

    private val dataFileWriter: ArrowWriter =
        runCatching { bufferPool.openArrowWriter(dataFilePath(tableName, trieKey), dataRel) }
            .onFailure { dataRel.close() }
            .getOrThrow()

    private val metaRel: Relation =
        runCatching { Relation(allocator, metaRelSchema) }
            .onFailure { dataRel.close(); dataFileWriter.close() }
            .getOrThrow()

    private val nodeWtr = metaRel["nodes"]!!
    private val nullBranchWtr = nodeWtr.legWriter("nil")

    private val iidBranchWtr = nodeWtr.legWriter("branch-iid")
    private val iidBranchElWtr = iidBranchWtr.elementWriter

    private val recencyBranchWtr = nodeWtr.legWriter("branch-recency")
    private val recencyElWtr = recencyBranchWtr.elementWriter
    private val recencyWtr = recencyBranchWtr.mapKeyWriter()
    private val recencyIdxWtr = recencyBranchWtr.mapValueWriter()

    private val leafWtr = nodeWtr.legWriter("leaf")
    private val pageIdxWtr = leafWtr.keyWriter("data-page-idx")

    private val pageMetaWriter =
        requiringResolve("xtdb.metadata/->page-meta-wtr")
            .invoke(leafWtr.keyWriter("columns"))
            .let { it as PageMetadataWriter }

    private var pageIdx = 0

    fun writeNull(): RowIndex {
        val pos = nodeWtr.valueCount
        nullBranchWtr.writeNull()
        return pos
    }

    fun writeLeaf(): RowIndex {
        val putReader = dataRel["op"]!!.legReader("put")
        val metaPos = nodeWtr.valueCount

        val temporalCols = listOf(
            dataRel["_system_from"],
            dataRel["_valid_from"],
            dataRel["_valid_to"],
            dataRel["_iid"]
        )

        val contentCols = writeContentMetadata.takeIf { it }
            ?.let { putReader?.keys?.mapNotNull { putReader.keyReader(it) } }
            .orEmpty()

        pageMetaWriter.writeMetadata(temporalCols + contentCols)

        pageIdxWtr.writeInt(pageIdx++)
        leafWtr.endStruct()
        metaRel.endRow()

        dataFileWriter.writePage()
        dataRel.clear()

        return metaPos
    }

    fun writeIidBranch(idxs: IntArray): Int {
        val rowIdx = nodeWtr.valueCount

        for (idx in idxs)
            if (idx < 0) iidBranchElWtr.writeNull() else iidBranchElWtr.writeInt(idx)

        iidBranchWtr.endList()
        metaRel.endRow()

        return rowIdx
    }

    fun writeRecencyBranch(idxBuckets: SortedMap<InstantMicros, RowIndex>): RowIndex {
        val rowIdx = nodeWtr.valueCount

        for ((recency, idx) in idxBuckets) {
            recencyWtr.writeLong(recency)
            recencyIdxWtr.writeInt(idx)
            recencyElWtr.endStruct()
        }

        recencyBranchWtr.endList()
        metaRel.endRow()

        return rowIdx
    }

    private fun writeLiveTrieNode(node: MemoryHashTrie.Node, dataRel: Relation) {
        val copier = this.dataRel.rowCopier(dataRel)

        fun MemoryHashTrie.Node.writeNode0(): Int =
            when (this) {
                is MemoryHashTrie.Branch -> {
                    val children = iidChildren

                    writeIidBranch(IntArray(children.size) { idx -> children[idx]?.writeNode0() ?: -1 })
                }

                is MemoryHashTrie.Leaf -> {
                    data.forEach { idx -> copier.copyRow(idx) }
                    writeLeaf()
                }
            }

        node.writeNode0()
    }

    private val metaFilePath = metaFilePath(tableName, trieKey)

    /**
     * @return the size of the data file
     */
    fun end(): FileSize {
        val dataFileSize = dataFileWriter.end()

        bufferPool.openArrowWriter(metaFilePath, metaRel).use { metaFileWriter ->
            metaFileWriter.writePage()
            metaFileWriter.end()
        }

        return dataFileSize
    }

    override fun close() {
        metaRel.close()
        dataFileWriter.close()
        dataRel.close()
    }
}
