package xtdb.api

import clojure.lang.IFn
import xtdb.api.log.LogFactory
import xtdb.api.storage.StorageFactory
import xtdb.util.requiringResolve
import java.time.Duration
import java.time.ZoneId
import java.time.ZoneOffset

object Xtdb {
    private val OPEN_NODE: IFn = requiringResolve("xtdb.node.impl", "open-node")

    class IndexerConfig(
        var logLimit: Long = 64L,
        var pageLimit: Long = 1024L,
        var rowsPerChunk: Long = 102400L,
        var flushDuration: Duration = Duration.ofHours(4),
    ) {
        fun logLimit(logLimit: Long) = apply { this.logLimit = logLimit }
        fun pageLimit(pageLimit: Long) = apply { this.pageLimit = pageLimit }
        fun rowsPerChunk(rowsPerChunk: Long) = apply { this.rowsPerChunk = rowsPerChunk }
        fun flushDuration(flushDuration: Duration) = apply { this.flushDuration = flushDuration }
    }

    class Config(
        txLog: LogFactory = LogFactory.DEFAULT,
        var storage: StorageFactory = StorageFactory.DEFAULT,
        defaultTz: ZoneId = ZoneOffset.UTC,
        @JvmField val indexer: IndexerConfig = IndexerConfig()
    ) : XtdbSubmitClient.Config(txLog, defaultTz) {
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

    interface Module : AutoCloseable

    interface ModuleFactory {
        val moduleKey: String

        fun openModule(xtdb: IXtdb): Module
    }

    @JvmStatic
    fun configure() = Config()

    @JvmStatic
    @JvmOverloads
    fun openNode(config: Config = Config()) = config.open()

    @JvmSynthetic
    fun openNode(build: Config.() -> Unit) = openNode(Config().also(build))
}
