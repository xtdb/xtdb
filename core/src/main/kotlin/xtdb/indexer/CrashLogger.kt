package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.util.asPath
import java.nio.file.Path
import xtdb.util.closeOnCatch
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.time.InstantSource
import java.time.temporal.ChronoUnit

private val LOG = CrashLogger::class.logger

class CrashLogger @JvmOverloads constructor(
    private val allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    private val nodeId: String,
    private val clock: InstantSource = InstantSource.system()
) {
    private fun writeArrow(path: Path, rel: RelationReader) {
        rel.openDirectSlice(allocator).use { slicedRel ->
            bufferPool.openArrowWriter(path, slicedRel).use { writer ->
                writer.writePage()
                writer.end()
            }
        }
    }

    private fun writeTxOpsToArrow(path: Path, txOpsRdr: VectorReader) {
        txOpsRdr.openDirectSlice(allocator).closeOnCatch { slicedVec ->
            Relation(allocator, listOf(slicedVec), txOpsRdr.valueCount).use { txOpsRel ->
                bufferPool.openArrowWriter(path, txOpsRel).use { writer ->
                    writer.writePage()
                    writer.end()
                }
            }
        }
    }

    fun writeCrashLog(
        crashEdn: String,
        table: TableRef,
        liveIndex: LiveIndex,
        liveTableTx: LiveTable.Tx,
        queryRel: RelationReader?,
        txOpsRdr: VectorReader?
    ) {
        val ts = clock.instant().truncatedTo(ChronoUnit.SECONDS).toString()
        val crashDir = "crashes/$nodeId/$ts".asPath

        LOG.warn("writing crash log: $crashDir")

        bufferPool.putObject(crashDir.resolve("crash.edn"), ByteBuffer.wrap(crashEdn.toByteArray()))

        val liveTable = liveIndex.liveTable(table)

        bufferPool.putObject(
            crashDir.resolve("live-trie.binpb"), 
            ByteBuffer.wrap(liveTable.liveTrie.asProto)
        )

        liveTable.liveRelation
            .takeIf { it.rowCount > 0 }
            ?.let { writeArrow(crashDir.resolve("live-table.arrow"), it) }

        writeArrow(crashDir.resolve("live-table-tx.arrow"), liveTableTx.txRelation)

        bufferPool.putObject(crashDir.resolve("live-trie-tx.binpb"), ByteBuffer.wrap(liveTableTx.transientTrie.asProto))

        queryRel?.let { writeArrow(crashDir.resolve("query-rel.arrow"), it) }
        txOpsRdr?.let { writeTxOpsToArrow(crashDir.resolve("tx-ops.arrow"), it) }

        LOG.info("crash log written: $crashDir")
    }
}
