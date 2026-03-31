package xtdb.debezium

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.subclass
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.ReplicaMessage
import xtdb.database.DatabaseState
import xtdb.database.ExternalLog
import xtdb.database.ExternalLog.MessageProcessor
import xtdb.database.proto.DatabaseConfig
import xtdb.debezium.proto.debeziumSourceConfig
import xtdb.indexer.Indexer.Companion.addTxRow
import xtdb.indexer.LeaderLogProcessor
import xtdb.indexer.LiveIndex
import xtdb.debezium.proto.DebeziumSourceConfig as DebeziumSourceConfigProto
import com.google.protobuf.Any as ProtoAny

private fun buildTableData(tx: LiveIndex.Tx): Map<String, ByteArray> =
    tx.liveTables.mapNotNull { (tableRef, tableTx) ->
        tableTx.serializeTxData()?.let { tableRef.schemaAndTable to it }
    }.toMap()

@Serializable
@SerialName("!Debezium")
data class DebeziumSource(val log: DebeziumLog.Factory) : ExternalLog.Factory {
    override fun open(clusters: Map<LogClusterAlias, Log.Cluster>) = log.openLog(clusters)

    override fun openProcessor(llp: LeaderLogProcessor, dbState: DatabaseState): MessageProcessor<DebeziumMessage> {
        val dbName = dbState.name
        val liveIndex = dbState.liveIndex

        return MessageProcessor { messages ->
            for (message in messages) {
                val token = ProtoAny.pack(message.offsets, "xtdb.debezium")
                // TODO: extract upstream txId from message (#5330)
                val txKey = TransactionKey(message.txId, message.systemTime)

                liveIndex.startTx(txKey).use { tx ->
                    for (event in message.ops) {
                        tx.indexCdcEvent(dbName, event, txKey)
                    }
                    tx.addTxRow(dbName, txKey, null)

                    val tableData = buildTableData(tx)
                    tx.commit()

                    llp.handleExternalTx(
                        ReplicaMessage.ResolvedTx(
                            txId = txKey.txId, systemTime = message.systemTime,
                            committed = true, error = null, tableData = tableData,
                            externalSourceToken = token,
                        )
                    )
                }
            }
        }
    }

    override fun writeTo(dbConfig: DatabaseConfig.Builder) {
        dbConfig.externalLog = ProtoAny.pack(debeziumSourceConfig {
            when (val l = log) {
                is KafkaDebeziumLog.Factory -> kafkaLog = l.toProto()
            }
        }, "proto.xtdb.com")
    }

    class Registration : ExternalLog.Registration {
        override val protoTag: String get() = "proto.xtdb.com/xtdb.debezium.proto.DebeziumSourceConfig"

        override fun fromProto(msg: ProtoAny): ExternalLog.Factory {
            val config = msg.unpack(DebeziumSourceConfigProto::class.java)
            return DebeziumSource(log = DebeziumLog.Factory.fromProto(config))
        }

        override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalLog.Factory>) {
            builder.subclass(DebeziumSource::class)
        }

        override val serializersModule: SerializersModule get() = DebeziumLog.Factory.serializersModule
    }
}
