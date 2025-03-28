package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.indexer.TrieMetadataCalculator
import xtdb.log.proto.TrieMetadata
import xtdb.metadata.PageMetadataWriter
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.Trie.metaRelSchema
import xtdb.util.requiringResolve

class MetadataFileWriter(
    al: BufferAllocator, private val bp: BufferPool,
    private val tableName: TableName, private val trieKey: TrieKey,
    private val dataRel: RelationReader,
    private val writeContentMetadata: Boolean, writeTrieMetadata: Boolean
) : AutoCloseable {

    private val iidVec = dataRel["_iid"]
    private val validFromVec = dataRel["_valid_from"]
    private val validToVec = dataRel["_valid_to"]
    private val systemFromVec = dataRel["_system_from"]

    private val opReader = dataRel["op"]
    private val putReader = opReader.vectorForOrNull("put")

    private val metaRel = Relation(al, metaRelSchema)

    private val nodeWtr = metaRel["nodes"]
    private val nullBranchWtr = nodeWtr["nil"]

    private val iidBranchWtr = nodeWtr["branch-iid"]
    private val iidBranchElWtr = iidBranchWtr.listElements

    private val leafWtr = nodeWtr["leaf"]
    private val pageIdxWtr = leafWtr["data-page-idx"]

    private val pageMetaWriter =
        requiringResolve("xtdb.metadata/->page-meta-wtr")
            .invoke(leafWtr["columns"])
            .let { it as PageMetadataWriter }

    private var pageIdx = 0

    private val trieMetaCalc =
        if (writeTrieMetadata) TrieMetadataCalculator(iidVec, validFromVec, validToVec, systemFromVec) else null

    fun writeNull(): RowIndex {
        val pos = nodeWtr.valueCount
        nullBranchWtr.writeNull()
        return pos
    }

    fun writeIidBranch(idxs: IntArray): Int {
        val rowIdx = nodeWtr.valueCount

        for (idx in idxs)
            if (idx < 0) iidBranchElWtr.writeNull() else iidBranchElWtr.writeInt(idx)

        iidBranchWtr.endList()
        metaRel.endRow()

        return rowIdx
    }

    fun writeLeaf(): RowIndex {
        val metaPos = nodeWtr.valueCount

        val temporalCols = listOf(systemFromVec, validFromVec, validToVec, iidVec)

        trieMetaCalc?.update(0, dataRel.rowCount)

        val contentCols = writeContentMetadata.takeIf { it }
            ?.let { putReader?.keyNames?.mapNotNull { putReader.vectorForOrNull(it) } }
            .orEmpty()

        pageMetaWriter.writeMetadata(temporalCols + contentCols)

        pageIdxWtr.writeInt(pageIdx++)
        leafWtr.endStruct()
        metaRel.endRow()

        return metaPos
    }

    fun end(): TrieMetadata {
        bp.openArrowWriter(tableName.metaFilePath(trieKey), metaRel)
            .use { metaFileWriter ->
                metaFileWriter.writePage()
                metaFileWriter.end()
            }

        return trieMetaCalc?.build()?.takeIf { it.rowCount > 0 } ?: TrieMetadata.newBuilder().build()
    }

    override fun close() = metaRel.close()
}