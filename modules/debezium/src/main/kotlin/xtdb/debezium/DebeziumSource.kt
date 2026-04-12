package xtdb.debezium

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.subclass
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.ExternalSource
import xtdb.database.proto.DatabaseConfig
import xtdb.debezium.proto.debeziumSourceConfig
import xtdb.debezium.proto.DebeziumSourceConfig as DebeziumSourceConfigProto
import com.google.protobuf.Any as ProtoAny

@Serializable
@SerialName("!Debezium")
data class DebeziumSource(
    val log: DebeziumLog.Factory,
    val messageFormat: MessageFormat,
) : ExternalSource.Factory {
    override fun open(dbName: String, clusters: Map<LogClusterAlias, Log.Cluster>) = log.openLog(dbName, clusters, messageFormat)

    override fun writeTo(dbConfig: DatabaseConfig.Builder) {
        dbConfig.externalSource = ProtoAny.pack(debeziumSourceConfig {
            when (val l = log) {
                is KafkaDebeziumLog.Factory -> kafkaLog = l.toProto()
            }
            if (messageFormat is MessageFormat.Avro) {
                avro = true
            }
        }, "proto.xtdb.com")
    }

    class Registration : ExternalSource.Registration {
        override val protoTag: String get() = "proto.xtdb.com/xtdb.debezium.proto.DebeziumSourceConfig"

        override fun fromProto(msg: ProtoAny): ExternalSource.Factory {
            val config = msg.unpack(DebeziumSourceConfigProto::class.java)
            val format = when (config.messageFormatCase) {
                DebeziumSourceConfigProto.MessageFormatCase.AVRO -> MessageFormat.Avro
                else -> MessageFormat.Json
            }
            return DebeziumSource(log = DebeziumLog.Factory.fromProto(config), messageFormat = format)
        }

        override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
            builder.subclass(DebeziumSource::class)
        }

        override val serializersModule: SerializersModule get() = DebeziumLog.Factory.serializersModule
    }
}
