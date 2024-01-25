@file:UseSerializers(ZoneIdSerde::class)
package xtdb.api

import clojure.lang.IFn
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.ZoneIdSerde
import xtdb.api.log.LogFactory
import xtdb.api.log.Logs.inMemoryLog
import xtdb.api.module.ModuleFactory
import xtdb.api.storage.Storage.inMemoryStorage
import xtdb.api.storage.StorageFactory
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
        override var txLog: LogFactory = inMemoryLog(),
        var storage: StorageFactory = inMemoryStorage(),
        override var defaultTz: ZoneId = ZoneOffset.UTC,
        @JvmField val indexer: IndexerConfig = IndexerConfig()
    ) : AConfig() {
        private val modules: MutableList<ModuleFactory> = mutableListOf()

        fun storage(storage: StorageFactory) = apply { this.storage = storage }

        fun getModules(): List<ModuleFactory> = modules
        fun module(module: ModuleFactory) = apply { this.modules += module }
        fun modules(vararg modules: ModuleFactory) = apply { this.modules += modules }
        fun modules(modules: List<ModuleFactory>) = apply { this.modules += modules }

        @JvmSynthetic
        fun indexer(configure: IndexerConfig.() -> Unit) = apply { indexer.configure() }

        override fun open() = OPEN_NODE.invoke(this) as IXtdb
    }

    @JvmStatic
    fun configure() = Config()

    @JvmStatic
    @JvmOverloads
    fun openNode(config: Config = Config()) = config.open()

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
