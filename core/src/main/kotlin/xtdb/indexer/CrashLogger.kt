package xtdb.indexer

import clojure.lang.Keyword
import clojure.lang.PersistentArrayMap
import clojure.lang.RT
import clojure.lang.Var
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.api.error.Anomaly
import xtdb.api.error.Anomaly.Companion.wrapAnomaly
import xtdb.storage.BufferPool
import xtdb.api.error.Interrupted
import xtdb.api.TableRef
import xtdb.util.asPath
import java.nio.file.Path
import xtdb.util.closeOnCatch
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.requiringResolve
import xtdb.util.warn
import java.io.StringWriter
import java.nio.ByteBuffer
import java.time.InstantSource
import java.time.temporal.ChronoUnit
import xtdb.api.tx.OpenTx

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
        openTxTable: OpenTx.Table,
        queryRel: RelationReader?,
        txOpsRdr: VectorReader?
    ) {
        val ts = clock.instant().truncatedTo(ChronoUnit.SECONDS).toString()
        val crashDir = "crashes/$nodeId/$ts".asPath

        LOG.warn("writing crash log: $crashDir")

        bufferPool.putObject(crashDir.resolve("crash.edn"), ByteBuffer.wrap(crashEdn.toByteArray()))

        val liveTable = liveIndex.table(table) ?: return

        bufferPool.putObject(
            crashDir.resolve("live-trie.binpb"),
            ByteBuffer.wrap(liveTable.liveTrie.asProto)
        )

        liveTable.liveRelation
            .takeIf { it.rowCount > 0 }
            ?.let { writeArrow(crashDir.resolve("live-table.arrow"), it) }

        writeArrow(crashDir.resolve("open-tx-table.arrow"), openTxTable.txRelation)

        bufferPool.putObject(crashDir.resolve("open-tx-trie.binpb"), ByteBuffer.wrap(openTxTable.trie.asProto))

        queryRel?.let { writeArrow(crashDir.resolve("query-rel.arrow"), it) }
        txOpsRdr?.let { writeTxOpsToArrow(crashDir.resolve("tx-ops.arrow"), it) }

        LOG.info("crash log written: $crashDir")
    }

    // Use clojure.pprint to preserve byte-for-byte the EDN header format the
    // original `crash-log!` helper produced. We rebind `*out*` to a StringWriter
    // via pushThreadBindings so pprint writes there instead of System.out.
    @PublishedApi
    internal fun crashEdn(ex: Throwable, data: Map<String, *>): String {
        val pprint = requiringResolve("clojure.pprint/pprint")
        val out = RT.`var`("clojure.core", "*out*")

        val entries = mutableMapOf<Any, Any?>()
        for ((k, v) in data) entries[Keyword.intern(k)] = v
        entries[Keyword.intern("ex")] = ex
        val map = PersistentArrayMap.create(entries)

        val sw = StringWriter()
        Var.pushThreadBindings(PersistentArrayMap.create(mapOf<Any, Any?>(out to sw)))
        try {
            pprint.invoke(map)
        } finally {
            Var.popThreadBindings()
        }
        return sw.toString()
    }

    inline fun <R> withCrashLog(
        msg: String,
        table: TableRef,
        liveIndex: LiveIndex,
        openTxTable: OpenTx.Table,
        queryRel: RelationReader? = null,
        txOpsRdr: VectorReader? = null,
        data: Map<String, *> = emptyMap<String, Any?>(),
        block: () -> R,
    ): R {
        try {
            return wrapAnomaly(data, block)
        } catch (e: Interrupted) {
            throw e
        } catch (e: Anomaly.Caller) {
            throw e
        } catch (e: Anomaly) {
            // crash-log-write failures must not mask the real error
            try {
                writeCrashLog(crashEdn(e, data + ("msg" to msg)), table, liveIndex, openTxTable, queryRel, txOpsRdr)
            } catch (t: Throwable) {
                e.addSuppressed(t)
            }
            throw e
        }
    }
}
