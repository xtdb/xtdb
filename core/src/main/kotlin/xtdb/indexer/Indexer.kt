package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.MessageId
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.query.IQuerySource
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.types.ClojureForm
import xtdb.util.asIid
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneId

private val LOG = Indexer::class.logger

interface Indexer : AutoCloseable {

    interface ForDatabase : AutoCloseable {
        fun indexTx(
            msgId: MessageId, msgTimestamp: Instant, txOps: VectorReader?,
            systemTime: Instant?, defaultTz: ZoneId?, user: String?, userMetadata: Any?
        ): ReplicaMessage.ResolvedTx

        fun addTxRow(txKey: TransactionKey, error: Throwable?): ReplicaMessage.ResolvedTx
    }

    companion object {
        @JvmStatic
        @JvmOverloads
        fun LiveIndex.Tx.addTxRow(
            dbName: DatabaseName,
            txKey: TransactionKey,
            error: Throwable?,
            userMetadata: Map<*, *>? = null
        ) {
            val txId = txKey.txId
            val systemTimeMicros = txKey.systemTime.asMicros

            val liveTable = liveTable(TableRef(dbName, "xt", "txs"))
            val docWriter = liveTable.docWriter

            liveTable.logPut(ByteBuffer.wrap(txId.asIid), systemTimeMicros, Long.MAX_VALUE) {
                docWriter.vectorFor("_id", VectorType.I64.arrowType, false)
                    .writeLong(txId)

                docWriter.vectorFor("system_time", VectorType.INSTANT.arrowType, false)
                    .writeLong(systemTimeMicros)

                docWriter.vectorFor("committed", VectorType.BOOL.arrowType, false)
                    .writeBoolean(error == null)

                docWriter.vectorFor("user_metadata", VectorType.structOf().arrowType, true)
                    .writeObject(userMetadata)

                val errorWriter = docWriter.vectorFor("error", VectorType.TRANSIT.arrowType, true)
                if (error == null) {
                    errorWriter.writeNull()
                } else {
                    try {
                        errorWriter.writeObject(error)
                    } catch (e: Exception) {
                        error.addSuppressed(e)
                        LOG.warn(error, "Error serializing error, tx $txId")
                        errorWriter.writeObject(ClojureForm("error serializing error - see server logs"))
                    }
                }

                docWriter.endStruct()
            }
        }

        @JvmStatic
        fun queryCatalog(
            storage: DatabaseStorage,
            state: DatabaseState,
            snapSource: Snapshot.Source
        ): IQuerySource.QueryCatalog {
            val queryDb = object : IQuerySource.QueryDatabase {
                override val storage get() = storage
                override val queryState get() = state
                override fun openSnapshot() = snapSource.openSnapshot()
            }
            return object : IQuerySource.QueryCatalog {
                override val databaseNames: Collection<DatabaseName> get() = setOf(state.name)
                override fun databaseOrNull(dbName: DatabaseName) = queryDb.takeIf { dbName == state.name }
            }
        }
    }

    fun openForDatabase(
        allocator: BufferAllocator,
        storage: DatabaseStorage,
        state: DatabaseState,
        liveIndex: LiveIndex,
        crashLogger: CrashLogger
    ): ForDatabase
}
