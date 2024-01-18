@file:UseSerializers(ZoneIdSerde::class)

package xtdb.api

import clojure.lang.IFn
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.ZoneIdSerde
import xtdb.api.log.LogFactory
import xtdb.util.requiringResolve
import java.time.ZoneId
import java.time.ZoneOffset

object XtdbSubmitClient {

    private val OPEN_CLIENT: IFn = requiringResolve("xtdb.node.impl", "open-submit-client")

    @Serializable
    data class Config(
        override var txLog: LogFactory = LogFactory.DEFAULT,
        override var defaultTz: ZoneId = ZoneOffset.UTC,
    ): AConfig() {
        override fun open() = OPEN_CLIENT(this) as IXtdbSubmitClient
    }

    @JvmStatic
    fun configure() = Xtdb.Config()

    @JvmStatic
    @JvmOverloads
    fun openSubmitClient(config: Config = Config()) = config.open()

    @JvmSynthetic
    fun openSubmitClient(build: Config.() -> Unit) = openSubmitClient(Config().also(build))
}
