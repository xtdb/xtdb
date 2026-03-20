package xtdb.debezium

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.subclass
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.ExternalLog
import xtdb.database.proto.DatabaseConfig
import xtdb.debezium.proto.debeziumSourceConfig
import xtdb.debezium.proto.DebeziumSourceConfig as DebeziumSourceConfigProto
import com.google.protobuf.Any as ProtoAny

@Serializable
@SerialName("!Debezium")
data class DebeziumSource(val log: DebeziumLog.Factory) : ExternalLog.Factory {
    override fun open(clusters: Map<LogClusterAlias, Log.Cluster>) = log.openLog(clusters)

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
