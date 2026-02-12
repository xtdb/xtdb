package xtdb.indexer

import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.LogOffset
import xtdb.api.log.MessageId
import xtdb.api.log.Watchers
import xtdb.database.Database
import xtdb.util.MsgIdUtil.msgIdToEpoch
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.util.MsgIdUtil.offsetToMsgId
import xtdb.util.logger
import xtdb.util.warn

private val LOG = ControlPlaneConsumer::class.logger

class ControlPlaneConsumer(
    private val dbCatalog: Database.Catalog,
    private val epoch: Int,
    latestProcessedMsgId: MessageId
) : Log.Subscriber, AutoCloseable {

    private val watchers = Watchers(latestProcessedMsgId)

    val latestProcessedOffset: LogOffset =
        if (latestProcessedMsgId >= 0 && msgIdToEpoch(latestProcessedMsgId) == epoch)
            msgIdToOffset(latestProcessedMsgId)
        else -1

    override fun processRecords(records: List<Log.Record>) {
        for (record in records) {
            val msgId = offsetToMsgId(epoch, record.logOffset)

            when (val msg = record.message) {
                is Message.AttachDatabase -> {
                    try { dbCatalog.attach(msg.dbName, msg.config) }
                    catch (e: Exception) { LOG.warn(e, "Failed to attach database '${msg.dbName}'") }
                }
                is Message.DetachDatabase -> {
                    try { dbCatalog.detach(msg.dbName) }
                    catch (e: Exception) { LOG.warn(e, "Failed to detach database '${msg.dbName}'") }
                }
                else -> {}
            }

            watchers.notify(msgId, null)
        }
    }

    fun awaitAsync(msgId: MessageId) = watchers.awaitAsync(msgId)

    override fun close() { watchers.close() }
}
