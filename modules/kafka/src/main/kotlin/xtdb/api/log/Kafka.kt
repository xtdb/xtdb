@file:UseSerializers(StringMapWithEnvVarsSerde::class, DurationSerde::class, StringWithEnvVarSerde::class, PathWithEnvVarSerde::class)
package xtdb.api.log

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.StringMapWithEnvVarsSerde
import xtdb.api.Xtdb
import xtdb.api.module.XtdbModule
import xtdb.util.requiringResolve
import java.nio.file.Path
import java.time.Duration

object Kafka {
    @JvmStatic
    fun kafka(bootstrapServers: String, txTopic: String, filesTopic: String) = Factory(bootstrapServers, txTopic, filesTopic)

    /**
     * Used to set configuration options for Kafka as an XTDB Transaction Log.
     *
     * For more info on setting up the necessary infrastructure to be able to use Kafka as an XTDB Transaction Log, see the
     * section on infrastructure within our [Kafka Module Reference](https://docs.xtdb.com/config/tx-log/kafka.html).
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    txLog = KafkaLogFactory(
     *              bootstrapServers = "localhost:9092",
     *              txTopic = "xtdb_txs",
     *              filesTopic = "xtdb_file_notifs",
     *              autoCreateTopics = true,
     *              txPollDuration = Duration.ofSeconds(1)
     *              filePollDuration = Duration.ofSeconds(5)
     *            ),
     *    ...
     * }
     * ```
     *
     * @property bootstrapServers A comma-separated list of host:port pairs to use for establishing the initial connection to the Kafka cluster.
     * @property txTopic Name of the Kafka topic to use for the transaction log.
     * @property filesTopic Name of the Kafka topic to use for the file notifications.
     * @property autoCreateTopics Whether to automatically create the topic, if it does not already exist.
     * @property txPollDuration The maximum amount of time to block waiting for TX records to be returned by the Kafka consumer.
     * @property filePollDuration The maximum amount of time to block waiting for file notifications to be returned by the Kafka consumer.
     * @property propertiesMap A map of Kafka connection properties, supplied directly to the Kafka client.
     * @property propertiesFile Path to a Java properties file containing Kafka connection properties, supplied directly to the Kafka client.
     */
    @Serializable
    @SerialName("!Kafka")
    data class Factory(
        val bootstrapServers: String,
        val txTopic: String,
        val filesTopic: String,
        var autoCreateTopics: Boolean = true,
        var txPollDuration: Duration = Duration.ofSeconds(1),
        var filePollDuration: Duration = Duration.ofSeconds(5),
        var propertiesMap: Map<String, String> = emptyMap(),
        var propertiesFile: Path? = null
    ) : Log.Factory {

        fun autoCreateTopics(autoCreateTopic: Boolean) = apply { this.autoCreateTopics = autoCreateTopic }
        fun txPollDuration(pollDuration: Duration) = apply { this.txPollDuration = pollDuration }
        fun filePollDuration(pollDuration: Duration) = apply { this.filePollDuration = pollDuration }
        fun propertiesMap(propertiesMap: Map<String, String>) = apply { this.propertiesMap = propertiesMap }
        fun propertiesFile(propertiesFile: Path) = apply { this.propertiesFile = propertiesFile }

        override fun openLog() = requiringResolve("xtdb.kafka/open-log")(this) as Log

    }

    /**
     * @suppress
     */
    class Registration: XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerLogFactory(Factory::class)
        }
    }
}

fun Xtdb.Config.kafka(bootstrapServers: String, txTopic: String, filesTopic: String, configure: Kafka.Factory.() -> Unit = {}) {
    txLog = Kafka.kafka(bootstrapServers, txTopic, filesTopic).also(configure)
}
