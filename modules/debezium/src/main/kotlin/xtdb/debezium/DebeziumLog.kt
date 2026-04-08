package xtdb.debezium

import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.ExternalLog
import xtdb.debezium.proto.DebeziumSourceConfig
import xtdb.debezium.proto.DebeziumSourceConfig.LogCase.*

sealed interface DebeziumLog : ExternalLog<DebeziumMessage> {

    interface Factory {
        fun openLog(dbName: String, clusters: Map<LogClusterAlias, Log.Cluster>, messageFormat: MessageFormat): DebeziumLog

        companion object {
            val serializersModule = SerializersModule {
                polymorphic(Factory::class) {
                    subclass(KafkaDebeziumLog.Factory::class)
                }
            }

            fun fromProto(config: DebeziumSourceConfig): Factory =
                when (config.logCase) {
                    KAFKA_LOG -> KafkaDebeziumLog.Factory.fromProto(config.kafkaLog)
                    else -> error("unknown debezium log: ${config.logCase}")
                }
        }
    }
}
