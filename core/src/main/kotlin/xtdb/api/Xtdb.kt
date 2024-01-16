package xtdb.api

import clojure.lang.IFn
import xtdb.api.log.LogFactory
import xtdb.api.storage.StorageFactory
import xtdb.util.requiringResolve

object Xtdb {
    private val OPEN_NODE: IFn = requiringResolve("xtdb.node.impl", "open-node")

    class IndexerConfig(var logLimit: Long = 64L, var pageLimit: Long = 1024L, var rowsPerChunk: Long = 102400L) {
        fun logLimit(logLimit: Long) = apply { this.logLimit = logLimit }
        fun pageLimit(pageLimit: Long) = apply { this.pageLimit = pageLimit }
        fun rowsPerChunk(rowsPerChunk: Long) = apply { this.rowsPerChunk = rowsPerChunk }
    }

    class Config(
        @JvmField val indexer: IndexerConfig = IndexerConfig(),
        var txLog: LogFactory = LogFactory.DEFAULT,
        var storage: StorageFactory = StorageFactory.DEFAULT,
        var extraConfig: Map<*, *> = emptyMap<Any, Any>(),
    ) {
        private val modules: MutableList<ModuleFactory> = mutableListOf()

        fun txLog(txLog: LogFactory) = apply { this.txLog = txLog }
        fun storage(storage: StorageFactory) = apply { this.storage = storage }

        fun getModules(): List<ModuleFactory> = modules
        fun module(module: ModuleFactory) = apply { this.modules += module }
        fun modules(vararg modules: ModuleFactory) = apply { this.modules += modules }
        fun modules(modules: List<ModuleFactory>) = apply { this.modules += modules }

        fun extraConfig(extraConfig: Map<*, *>) = apply { this.extraConfig = extraConfig }

        @JvmSynthetic
        fun indexer(configure: IndexerConfig.() -> Unit) = apply { indexer.configure() }

        fun open() = OPEN_NODE(this) as IXtdb
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
    fun openNode(config: Config = Config()): IXtdb {
        return OPEN_NODE(config) as IXtdb
    }

    @JvmSynthetic
    fun openNode(build: Config.() -> Unit): IXtdb {
        return openNode(Config().also(build))
    }
}
