package xtdb.indexer

import io.micrometer.core.instrument.Timer
import io.micrometer.tracing.Tracer
import org.apache.arrow.memory.BufferAllocator
import xtdb.Metrics.withSpan
import xtdb.NodeBase
import xtdb.api.TransactionKey
import xtdb.arrow.*
import xtdb.arrow.Vector.Companion.openVector
import xtdb.api.DatabaseName
import xtdb.database.PartitionState
import xtdb.api.error.Anomaly
import xtdb.api.error.Anomaly.Companion.wrapAnomaly
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import xtdb.api.query.QueryOpts
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.api.TableRef
import xtdb.table.fromSchemaAndTable
import xtdb.trie.InstantMicros
import xtdb.util.asIid
import xtdb.util.closeOnCatch
import java.time.Instant
import java.time.ZoneId
import xtdb.api.tx.OpenTx

private val FORBIDDEN_SCHEMAS = setOf("xt", "information_schema", "pg_catalog")

private fun checkNotForbidden(table: TableRef) {
    if (table.schemaName in FORBIDDEN_SCHEMAS) {
        throw Incorrect(
            "Cannot write to table: ${table.schemaAndTable}",
            "xtdb/forbidden-table",
            mapOf("table" to table),
        )
    }
}

private fun assertTimestampColType(rdr: VectorReader) {
    if (rdr.arrowType != VectorType.INSTANT.arrowType) {
        throw Fault(
            "Invalid timestamp col type",
            "xtdb/invalid-timestamp-col-type",
            mapOf("field" to rdr.field),
        )
    }
}

private fun VectorReader.openRange(al: BufferAllocator, name: String, startIdx: Int, count: Int): Vector =
    al.openVector(name, type).closeOnCatch { it.apply { appendRange(this@openRange, startIdx, count) } }

internal class SourceLogTxIndexer(
    private val allocator: BufferAllocator,
    base: NodeBase,
    private val partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val crashLogger: CrashLogger,
) {

    private val liveIndex = partitionState.liveIndex

    private val tracer: Tracer? =
        if (base.config.tracer.transactionTracing) base.tracer else null

    private val txTimer: Timer? = base.meterRegistry?.let { reg ->
        Timer.builder("tx.op.timer")
            .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
            .description("indicates the timing and number of transactions")
            .register(reg)
    }

    internal data class TxOpts(
        val txKey: TransactionKey,
        val currentTime: Instant,
        val systemTime: InstantMicros,
        val defaultTz: ZoneId?,
        val user: String?,
    )

    internal inner class ForTx(private val txOpsRdr: VectorReader, private val opts: TxOpts) {

        private fun OpenTx.applyPutDocs(opIdx: Int) {
            val putLeg = txOpsRdr.vectorFor("put-docs")
            val iidsRdr = putLeg.vectorFor("iids")
            val iidRdr = iidsRdr.listElements
            val docsRdr = putLeg.vectorFor("documents")
            val validFromRdr = putLeg.vectorFor("_valid_from")
            val validToRdr = putLeg.vectorFor("_valid_to")

            val legName = docsRdr.getLeg(opIdx) ?: error("put-docs leg missing for op $opIdx")
            val table = fromSchemaAndTable(legName)
            checkNotForbidden(table)

            val tableDocsRdr = docsRdr.vectorFor(legName)
            val docRdr = tableDocsRdr.listElements
            val ks = docRdr.keyNames ?: emptySet()

            val forbiddenCols = ks.filter {
                it.startsWith("_") && it !in setOf("_id", "_fn", "_valid_from", "_valid_to")
            }
            if (forbiddenCols.isNotEmpty()) {
                throw Incorrect(
                    "Cannot put documents with columns: ${forbiddenCols.toSet()}",
                    "xtdb/forbidden-columns",
                    mapOf("table" to table, "forbidden-cols" to forbiddenCols),
                )
            }

            val docStartIdx = tableDocsRdr.getListStartIndex(opIdx)
            val docCount = tableDocsRdr.getListCount(opIdx)

            val docCols = ks.filter { it != "_valid_from" && it != "_valid_to" }.associateWith { docRdr.vectorFor(it) }

            val idRdr = if (iidsRdr.isNull(opIdx)) docRdr.vectorFor("_id") else null

            val rowVfRdr = docRdr.vectorForOrNull("_valid_from")?.also { assertTimestampColType(it) }
            val rowVtRdr = docRdr.vectorForOrNull("_valid_to")?.also { assertTimestampColType(it) }

            val validFrom = if (validFromRdr.isNull(opIdx)) opts.systemTime else validFromRdr.getLong(opIdx)
            val validTo = if (validToRdr.isNull(opIdx)) Long.MAX_VALUE else validToRdr.getLong(opIdx)

            val liveTable = table(table)
            crashLogger.withCrashLog(
                "error putting documents",
                table = table,
                liveIndex = liveIndex,
                openTxTable = liveTable,
                txOpsRdr = txOpsRdr,
                data = mapOf("table" to table, "tx-key" to opts.txKey, "tx-op-idx" to opIdx),
            ) {
                if (docCount == 0) return@withCrashLog

                if (idRdr != null)
                    repeat(docCount) { liveTable.writeIid(idRdr.getObject(docStartIdx + it).asIid) }
                else
                    liveTable.writeIids(iidRdr, iidsRdr.getListStartIndex(opIdx), docCount)

                liveTable.writeValidTimes(rowVfRdr, rowVtRdr, docStartIdx, docCount, validFrom, validTo)

                val putDocWriter = liveTable.putDocWriter
                for ((k, col) in docCols)
                    putDocWriter.vectorFor(k, col.arrowType, col.nullable).appendRange(col, docStartIdx, docCount)
                repeat(docCount) { putDocWriter.endStruct() }

                liveTable.endPuts(docCount)
            }
        }


        private fun OpenTx.applyDeleteDocs(opIdx: Int) {
            val deleteLeg = txOpsRdr.vectorFor("delete-docs")
            val tableRdr = deleteLeg.vectorFor("table")
            val iidsRdr = deleteLeg.vectorFor("iids")
            val iidRdr = iidsRdr.listElements
            val validFromRdr = deleteLeg.vectorFor("_valid_from")
            val validToRdr = deleteLeg.vectorFor("_valid_to")

            val table = fromSchemaAndTable(tableRdr.getObject(opIdx) as String)
            checkNotForbidden(table)

            val openTxTable = table(table)
            crashLogger.withCrashLog(
                "error deleting documents",
                table = table,
                liveIndex = liveIndex,
                openTxTable = openTxTable,
                txOpsRdr = txOpsRdr,
                data = mapOf("table" to table, "tx-key" to opts.txKey, "tx-op-idx" to opIdx),
            ) {
                val validFrom = if (validFromRdr.isNull(opIdx)) opts.systemTime else validFromRdr.getLong(opIdx)
                val validTo = if (validToRdr.isNull(opIdx)) Long.MAX_VALUE else validToRdr.getLong(opIdx)
                val iidCount = iidsRdr.getListCount(opIdx)
                if (iidCount == 0) return@withCrashLog

                openTxTable.writeIids(iidRdr, iidsRdr.getListStartIndex(opIdx), iidCount)
                openTxTable.writeValidTimes(null, null, 0, iidCount, validFrom, validTo)
                openTxTable.endDeletes(iidCount)
            }
        }

        private fun OpenTx.applyEraseDocs(opIdx: Int) {
            val eraseLeg = txOpsRdr.vectorFor("erase-docs")
            val tableRdr = eraseLeg.vectorFor("table")
            val iidsRdr = eraseLeg.vectorFor("iids")
            val iidRdr = iidsRdr.listElements

            val table = fromSchemaAndTable(tableRdr.getObject(opIdx) as String)
            checkNotForbidden(table)

            val liveTable = table(table)
            crashLogger.withCrashLog(
                "error erasing documents",
                table = table,
                liveIndex = liveIndex,
                openTxTable = liveTable,
                txOpsRdr = txOpsRdr,
                data = mapOf("table" to table, "tx-key" to opts.txKey, "tx-op-idx" to opIdx),
            ) {
                val iidCount = iidsRdr.getListCount(opIdx)
                if (iidCount == 0) return@withCrashLog

                liveTable.writeIids(iidRdr, iidsRdr.getListStartIndex(opIdx), iidCount)
                repeat(iidCount) { liveTable.writeValidTimeMicros(Long.MIN_VALUE, Long.MAX_VALUE) }
                liveTable.endErases(iidCount)
            }
        }

        private fun OpenTx.applyPatchDocs(opIdx: Int) {
            val patchLeg = txOpsRdr.vectorFor("patch-docs")
            val iidsRdr = patchLeg.vectorFor("iids")
            val iidRdr = iidsRdr.listElements
            val docsRdr = patchLeg.vectorFor("documents")
            val validFromRdr = patchLeg.vectorFor("_valid_from")
            val validToRdr = patchLeg.vectorFor("_valid_to")

            val legName = docsRdr.getLeg(opIdx) ?: error("patch-docs leg missing for op $opIdx")
            val tableRef = fromSchemaAndTable(legName)
            val tableDocsRdr = docsRdr.vectorFor(legName)
            val docRdr = tableDocsRdr.listElements
            val ks = docRdr.keyNames ?: emptySet()

            val forbiddenCols = ks.filter { it.startsWith("_") && it !in setOf("_id", "_fn") }
            if (forbiddenCols.isNotEmpty()) {
                throw Incorrect(
                    "Cannot patch documents with columns: ${forbiddenCols.toSet()}",
                    "xtdb/forbidden-columns",
                    mapOf("table" to tableRef, "forbidden-cols" to forbiddenCols),
                )
            }

            wrapAnomaly(mapOf("tx-op-idx" to opIdx, "tx-key" to opts.txKey)) {
                // Arrow-null `_valid_from` on a patch-docs tx-op means "from now" (matches put-docs /
                // delete-docs). SQL `PATCH ... FROM NULL` doesn't go through this path as Arrow-null —
                // the SQL planner writes the start-of-time µs sentinel explicitly (see sql.clj
                // #visitPatchStatementValidTimePortion).
                val validFrom: InstantMicros =
                    if (validFromRdr.isNull(opIdx)) opts.systemTime else validFromRdr.getLong(opIdx)

                val validTo: InstantMicros =
                    if (validToRdr.isNull(opIdx)) Long.MAX_VALUE else validToRdr.getLong(opIdx)

                val iidCount = iidsRdr.getListCount(opIdx)

                iidRdr.openRange(allocator, "_iid", iidsRdr.getListStartIndex(opIdx), iidCount).use { iids ->
                    docRdr.openRange(allocator, "doc", tableDocsRdr.getListStartIndex(opIdx), tableDocsRdr.getListCount(opIdx))
                        .use { docVec ->
                            val docs = RelationReader.from(listOf(iids, docVec), iidCount)

                            tracer.withSpan("xtdb.transaction.patch-docs") { span ->
                                span?.tag("db", dbName)
                                span?.tag("schema", tableRef.schemaName)
                                span?.tag("table", tableRef.tableName)
                                table(tableRef).patchDocs(docs, validFrom, validTo)
                            }
                        }
                }
            }
        }

        private inline fun forEachArgRow(loader: Relation.ILoader?, evalQuery: (RelationReader?) -> Unit) {
            if (loader == null) {
                evalQuery(null)
                return
            }

            Relation(allocator, loader.schema).use { paramRel ->
                while (loader.loadNextPage(paramRel)) {
                    val selection = IntArray(1)
                    for (idx in 0 until paramRel.rowCount) {
                        wrapAnomaly(mapOf("arg-idx" to idx)) {
                            selection[0] = idx
                            paramRel.select(selection).openSlice(allocator)
                                .closeOnCatch { evalQuery(it) }
                        }
                    }
                }
            }
        }

        private fun OpenTx.applySql(opIdx: Int) {
            val sqlLeg = txOpsRdr.vectorFor("sql")
            val queryRdr = sqlLeg.vectorFor("query")
            val argsRdr = sqlLeg.vectorFor("args")
            val qOpts = QueryOpts(opts.currentTime, opts.defaultTz)

            val queryStr = queryRdr.getObject(opIdx) as String

            wrapAnomaly(mapOf("sql" to queryStr, "tx-op-idx" to opIdx, "tx-key" to opts.txKey)) {
                val argsLoader: Relation.ILoader? =
                    if (argsRdr.isNull(opIdx)) null
                    else Relation.streamLoader(allocator, argsRdr.getObject(opIdx) as ByteArray)

                argsLoader.use {
                    tracer.withSpan("xtdb.transaction.sql") { span ->
                        span?.tag("query.text", queryStr)
                        forEachArgRow(argsLoader) { args ->
                            executeSql(queryStr, args, qOpts, opts.user)
                        }
                    }
                }
            }
        }

        private fun OpenTx.applyOp(opIdx: Int) {
            when (val leg = txOpsRdr.getLeg(opIdx)) {
                "sql" -> applySql(opIdx)
                "put-docs" -> applyPutDocs(opIdx)
                "patch-docs" -> applyPatchDocs(opIdx)
                "delete-docs" -> applyDeleteDocs(opIdx)
                "erase-docs" -> applyEraseDocs(opIdx)

                "xtql" -> throw Unsupported(
                    buildString {
                        append("XTQL DML is no longer supported, as of 2.0.0-beta7. ")
                        append("Please use SQL DML statements instead - see the release notes for more information.")
                    },
                    "xtdb/xtql-dml-removed",
                )

                "call" -> throw Unsupported(
                    buildString {
                        append("tx-fns are no longer supported, as of 2.0.0-beta7. ")
                        append("Please use ASSERTs and SQL DML statements instead - see the release notes for more information.")
                    },
                    "xtdb/tx-fns-removed",
                )

                else -> error("unexpected tx-op leg: $leg")
            }
        }

        private inline fun recordTimed(crossinline block: () -> Unit) {
            if (txTimer != null) txTimer.recordCallable<Unit> { block() } else block()
        }

        // userMetadata in TxResult is what *the writer* chose post-hoc; the source-log path's
        // metadata is determined upstream by the message header, so the orchestration attaches it
        // at commit time rather than threading it through here.
        fun indexTx(openTx: OpenTx): TxResult =
            try {
                wrapAnomaly {
                    for (n in 0 until txOpsRdr.valueCount)
                        recordTimed { openTx.applyOp(n) }

                    TxResult.Committed()
                }
            } catch (e: Anomaly.Caller) {
                TxResult.Aborted(e)
            }
    }
}
