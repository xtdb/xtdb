@file:UseSerializers(ZoneIdSerde::class)

package xtdb.api

import clojure.lang.IFn
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.ZoneIdSerde
import xtdb.api.log.LogFactory
import xtdb.api.log.Logs.inMemoryLog
import xtdb.util.requiringResolve
import java.nio.file.Files
import java.nio.file.Path
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.io.path.extension

object XtdbSubmitClient {

    private val OPEN_CLIENT: IFn = requiringResolve("xtdb.node.impl", "open-submit-client")

    @Serializable
    data class Config(
        override var txLog: LogFactory = inMemoryLog(),
        override var defaultTz: ZoneId = ZoneOffset.UTC,
    ): AConfig() {
        override fun open() = OPEN_CLIENT(this) as IXtdbSubmitClient
    }

    @JvmStatic
    fun configure() = Config()

    @JvmStatic
    @JvmOverloads
    fun openSubmitClient(config: Config = Config()) = config.open()

    @JvmStatic
    fun openSubmitClient(path: Path): IXtdbSubmitClient {
        if (path.extension != "yaml") {
            throw IllegalArgumentException("Invalid config file type - must be '.yaml'")
        } else if (!path.toFile().exists()) {
            throw IllegalArgumentException("Provided config file does not exist")
        }

        val yamlString = Files.readString(path)
        val config = submitClient(yamlString)

        return config.open()
    }
    
    @JvmSynthetic
    fun openSubmitClient(build: Config.() -> Unit) = openSubmitClient(Config().also(build))
}
