@file:UseSerializers(ZoneIdSerde::class)
package xtdb.api

import clojure.lang.IFn
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.ZoneIdSerde
import xtdb.api.log.Log
import xtdb.api.log.Logs.inMemoryLog
import xtdb.api.module.XtdbModule
import xtdb.api.storage.Storage.inMemoryStorage
import xtdb.api.storage.Storage
import xtdb.util.requiringResolve
import java.nio.file.Files
import java.nio.file.Path
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.io.path.extension

object Xtdb {
    private val OPEN_NODE: IFn = requiringResolve("xtdb.node.impl", "open-node")

    @Serializable
    data class Config(
        var txLog: Log.Factory = inMemoryLog(),
        var storage: Storage.Factory = inMemoryStorage(),
        var defaultTz: ZoneId = ZoneOffset.UTC,
        @JvmField val indexer: IndexerConfig = IndexerConfig()
    ) {
        private val modules: MutableList<XtdbModule.Factory> = mutableListOf()

        fun storage(storage: Storage.Factory) = apply { this.storage = storage }

        fun getModules(): List<XtdbModule.Factory> = modules
        fun module(module: XtdbModule.Factory) = apply { this.modules += module }
        fun modules(vararg modules: XtdbModule.Factory) = apply { this.modules += modules }
        fun modules(modules: List<XtdbModule.Factory>) = apply { this.modules += modules }

        @JvmSynthetic
        fun indexer(configure: IndexerConfig.() -> Unit) = apply { indexer.configure() }

        fun open() = OPEN_NODE.invoke(this) as IXtdb
        fun txLog(txLog: Log.Factory) = apply { this.txLog = txLog }
        fun defaultTz(defaultTz: ZoneId) = apply { this.defaultTz = defaultTz }
    }

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
    fun openNode(path: Path): IXtdb {
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
