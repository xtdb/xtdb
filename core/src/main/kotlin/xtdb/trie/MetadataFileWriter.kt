package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.indexer.TrieMetadataCalculator
import xtdb.log.proto.TrieMetadata
import xtdb.metadata.ColumnMetadata
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.Trie.metaFilePath
import xtdb.types.Type
import xtdb.types.Type.Companion.BOOL
import xtdb.types.Type.Companion.I32
import xtdb.types.Type.Companion.I64
import xtdb.types.Type.Companion.NULL
import xtdb.types.Type.Companion.UTF8
import xtdb.types.Type.Companion.asListOf
import xtdb.types.Type.Companion.asStructOf
import xtdb.types.Type.Companion.asUnionOf
import xtdb.types.Type.Companion.listTypeOf
import xtdb.types.Type.Companion.maybe
import xtdb.types.Type.Companion.ofType
import xtdb.types.schema

class MetadataFileWriter(
    al: BufferAllocator, private val bp: BufferPool,
    private val table: TableRef, private val trieKey: TrieKey,
    private val dataRel: RelationReader,
    calculateBlooms: Boolean, writeTrieMetadata: Boolean
) : AutoCloseable {
    companion object {
        private val metadataField = listTypeOf(
            Type.structOf(
                "col-name" ofType UTF8,
                "root-col?" ofType BOOL,
                "count" ofType I64
            ),
            elName = "col"
        )

        @JvmField
        val metaRelSchema = schema(
            "nodes".asUnionOf(
                "nil" ofType NULL,
                "branch-iid" asListOf maybe(I32),
                "leaf".asStructOf(
                    "data-page-idx" ofType I32,
                    "columns" ofType metadataField
                )
            )
        )
    }

    private val iidVec = dataRel["_iid"]
    private val validFromVec = dataRel["_valid_from"]
    private val validToVec = dataRel["_valid_to"]
    private val systemFromVec = dataRel["_system_from"]

    private val opReader = dataRel["op"]
    private val putReader = opReader.vectorForOrNull("put")

    private val metaRel = Relation.open(al, metaRelSchema)

    private val nodeWtr = metaRel["nodes"]
    private val nullBranchWtr = nodeWtr["nil"]

    private val iidBranchWtr = nodeWtr["branch-iid"]
    private val iidBranchElWtr = iidBranchWtr.listElements

    private val leafWtr = nodeWtr["leaf"]
    private val pageIdxWtr = leafWtr["data-page-idx"]

    private val colsVec = leafWtr["columns"]
    private val colMetaWriter = ColumnMetadata(colsVec.listElements, calculateBlooms)

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

        colMetaWriter.writeMetadata(iidVec)
        colMetaWriter.writeMetadata(validFromVec)
        colMetaWriter.writeMetadata(validToVec)
        colMetaWriter.writeMetadata(systemFromVec)

        trieMetaCalc?.update(0, dataRel.rowCount)

        for (contentCol in putReader?.keyNames?.mapNotNull { putReader.vectorForOrNull(it) }.orEmpty()) {
            colMetaWriter.writeMetadata(contentCol)
        }

        colsVec.endList()

        pageIdxWtr.writeInt(pageIdx++)
        leafWtr.endStruct()
        metaRel.endRow()

        return metaPos
    }

    fun end(): TrieMetadata {
        bp.openArrowWriter(table.metaFilePath(trieKey), metaRel)
            .use { metaFileWriter ->
                metaFileWriter.writePage()
                metaFileWriter.end()
            }

        return trieMetaCalc?.build()?.takeIf { it.rowCount > 0 } ?: TrieMetadata.newBuilder().build()
    }

    override fun close() = metaRel.close()
}