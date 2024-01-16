package xtdb.api

import clojure.lang.IFn
import xtdb.api.log.LogFactory
import xtdb.util.requiringResolve
import java.time.ZoneId
import java.time.ZoneOffset

object XtdbSubmitClient {

    private val OPEN_CLIENT: IFn = requiringResolve("xtdb.node.impl", "open-submit-client")

    open class Config(
        var txLog: LogFactory = LogFactory.DEFAULT,
        var defaultTz: ZoneId = ZoneOffset.UTC,
    ) {
        fun txLog(txLog: LogFactory) = apply { this.txLog = txLog }
        fun defaultTz(defaultTz: ZoneId) = apply { this.defaultTz = defaultTz }
        open fun open() = OPEN_CLIENT(this) as IXtdbSubmitClient
    }

    @JvmStatic
    fun configure() = Xtdb.Config()

    @JvmStatic
    @JvmOverloads
    fun openSubmitClient(config: Config = Config()) = config.open()

    @JvmSynthetic
    fun openSubmitClient(build: Config.() -> Unit) = openSubmitClient(Config().also(build))
}
