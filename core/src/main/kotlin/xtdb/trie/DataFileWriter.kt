package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.ArrowWriter
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.trie.Trie.dataFilePath

class DataFileWriter(
    al: BufferAllocator, private val bp: BufferPool,
    private val tableName: TableName, private val trieKey: TrieKey, dataSchema: Schema,
) : AutoCloseable {
    val dataRel: Relation = Relation.open(al, dataSchema)

    private val dataFileWriter: ArrowWriter =
        runCatching { bp.openArrowWriter(tableName.dataFilePath(trieKey), dataRel) }
            .onFailure { dataRel.close() }
            .getOrThrow()

    fun writePage() {
        dataFileWriter.writePage()
        dataRel.clear()
    }

    fun end(): FileSize = dataFileWriter.end()

    override fun close() {
        dataFileWriter.close()
        dataRel.close()
    }
}