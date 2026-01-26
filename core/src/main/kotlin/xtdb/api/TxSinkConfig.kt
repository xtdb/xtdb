package xtdb.api
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.log.Log

@Serializable
data class TxSinkConfig(
    var dbName: String = "xtdb",
    var format: String = "transit+json",
    var outputLog: Log.Factory = Log.inMemoryLog,
    var initialScan: Boolean = false,
) {
    // NOTE: Intentionally not serialized to prevent accidental enabling in cluster config
    var enable: Boolean = false

    fun enable(enable: Boolean = true) = apply { this.enable = enable }
    fun initialScan(initialScan: Boolean) = apply { this.initialScan = initialScan }
    fun dbName(dbName: String) = apply { this.dbName = dbName }
    fun format(format: String) = apply { this.format = format }
    fun outputLog(log: Log.Factory) = apply { this.outputLog = log }
}
