package xtdb.api

import xtdb.api.log.Log.Factory
import java.time.ZoneId

/**
 * @suppress
 */
abstract class AConfig {
    abstract var txLog: Factory
    abstract var defaultTz: ZoneId

    fun txLog(txLog: Factory) = apply { this.txLog = txLog }
    fun defaultTz(defaultTz: ZoneId) = apply { this.defaultTz = defaultTz }
    abstract fun open(): IXtdbSubmitClient
}
