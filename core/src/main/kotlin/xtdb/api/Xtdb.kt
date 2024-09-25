@file:UseSerializers(ZoneIdSerde::class)

package xtdb.api

import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.ZoneIdSerde
import xtdb.api.log.Log
import xtdb.api.log.Logs.inMemoryLog
import xtdb.api.metrics.Metrics
import xtdb.api.module.XtdbModule
import xtdb.api.storage.Storage
import xtdb.api.storage.Storage.inMemoryStorage
import xtdb.util.requiringResolve
import java.nio.file.Files
import java.nio.file.Path
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.io.path.extension

interface Xtdb : AutoCloseable {

    val serverPort: Int

    @Serializable
    data class Config(
        var server : ServerConfig? = ServerConfig(),
        var txLog: Log.Factory = inMemoryLog(),
        var storage: Storage.Factory = inMemoryStorage(),
        var metrics: Metrics.Factory? = null,
        var defaultTz: ZoneId = ZoneOffset.UTC,
        @JvmField val indexer: IndexerConfig = IndexerConfig()
    ) {
        private val modules: MutableList<XtdbModule.Factory> = mutableListOf()

        fun txLog(txLog: Log.Factory) = apply { this.txLog = txLog }
        fun storage(storage: Storage.Factory) = apply { this.storage = storage }

        @JvmSynthetic
        fun server(configure: ServerConfig.() -> Unit) = apply { (server ?: ServerConfig()).configure() }

        @JvmSynthetic
        fun indexer(configure: IndexerConfig.() -> Unit) = apply { indexer.configure() }

        fun defaultTz(defaultTz: ZoneId) = apply { this.defaultTz = defaultTz }

        @JvmSynthetic
        fun metrics(metrics: Metrics.Factory) = apply { this.metrics = metrics }

        fun getModules(): List<XtdbModule.Factory> = modules
        fun module(module: XtdbModule.Factory) = apply { this.modules += module }
        fun modules(vararg modules: XtdbModule.Factory) = apply { this.modules += modules }
        fun modules(modules: List<XtdbModule.Factory>) = apply { this.modules += modules }

        fun open(): Xtdb = requiringResolve("xtdb.node.impl/open-node").invoke(this) as Xtdb
    }

    companion object {

        @JvmStatic
        fun configure() = Config()

        @JvmStatic
        @JvmOverloads
        fun openNode(config: Config = Config()) = config.open()

        /**
         * Opens a node using a YAML configuration file - will throw an exception if the specified path does not exist
         * or is not a valid `.yaml` extension file.
         */
        @JvmStatic
        fun openNode(path: Path): Xtdb {
            if (path.extension != "yaml") {
                throw IllegalArgumentException("Invalid config file type - must be '.yaml'")
            } else if (!path.toFile().exists()) {
                throw IllegalArgumentException("Provided config file does not exist")
            }

            val yamlString = Files.readString(path)
            val config = nodeConfig(yamlString)

            return config.open()
        }

        @JvmSynthetic
        fun openNode(configure: Config.() -> Unit) = openNode(Config().also(configure))
    }
}
