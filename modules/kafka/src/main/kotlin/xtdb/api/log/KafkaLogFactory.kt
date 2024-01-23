@file:UseSerializers(DurationSerde::class)
package xtdb.api.log

import clojure.lang.IFn
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.*
import xtdb.util.requiringResolve
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Duration
import java.time.InstantSource
import java.util.concurrent.CompletableFuture

@Serializable
@SerialName("!Kafka")
data class KafkaLogFactory @JvmOverloads constructor(
    @Serializable(with = StringWithEnvVarSerde::class) val bootstrapServers: String,
    @Serializable(with = StringWithEnvVarSerde::class) val topicName: String,
    var autoCreateTopic: Boolean = true,
    var replicationFactor: Int = 1,
    var pollDuration: Duration = Duration.ofSeconds(1),
    var topicConfig: Map<String, String> = emptyMap<String, String>(),
    var propertiesMap: Map<String, String> = emptyMap<String, String>(),
    @Serializable(with = PathWithEnvVarSerde::class) var propertiesFile: Path? = null
) : LogFactory {

    companion object {
        private val OPEN_LOG: IFn = requiringResolve("xtdb.kafka", "open-log")
    }

    fun autoCreateTopic(autoCreateTopic: Boolean) = apply { this.autoCreateTopic = autoCreateTopic }
    fun replicationFactor(replicationFactor: Int) = apply { this.replicationFactor = replicationFactor }
    fun pollDuration(pollDuration: Duration) = apply { this.pollDuration = pollDuration }
    fun topicConfig(topicConfig: Map<String, String>) = apply { this.topicConfig = topicConfig }
    fun propertiesMap(propertiesMap: Map<String, String>) = apply { this.propertiesMap = propertiesMap }
    fun propertiesFile(propertiesFile: Path) = apply { this.propertiesFile = propertiesFile }

    override fun openLog() = OPEN_LOG(this) as Log

    class Registration: ModuleRegistration {
        override fun register(registry: ModuleRegistry) {
            registry.registerLogFactory(KafkaLogFactory::class)
        }
    }
}
