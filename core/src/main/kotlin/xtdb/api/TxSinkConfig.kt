package xtdb.api
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.log.Log

@Serializable
data class TxSinkConfig(
    var format: String = "json",
    var outputLog: Log.Factory = Log.inMemoryLog
) {
    fun format(format: String) = apply { this.format = format }
    fun outputLog(log: Log.Factory) = apply { this.outputLog = log }
}
