package xtdb.api

import xtdb.api.log.LogFactory
import java.time.ZoneId

abstract class AConfig {
    abstract var txLog: LogFactory
    abstract var defaultTz: ZoneId

    fun txLog(txLog: LogFactory) = apply { this.txLog = txLog }
    fun defaultTz(defaultTz: ZoneId) = apply { this.defaultTz = defaultTz }
    abstract fun open(): IXtdbSubmitClient
}
