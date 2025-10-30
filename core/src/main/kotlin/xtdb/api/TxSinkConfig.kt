package xtdb.api
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.log.Log

@Serializable
data class TxSinkConfig(
    var format: String = "json",
    var outputLog: Log.Factory = Log.inMemoryLog,
    var tableFilter: TableFilter = TableFilter(),
) {
    @Serializable
    data class TableFilter (
        var include: Set<String> = emptySet(),
        var exclude: Set<String> = emptySet(),
    ) {
        fun test(tableName: String): Boolean =
            tableName !in exclude && (include.isEmpty() || tableName in include)
    }

    fun format(format: String) = apply { this.format = format }
    fun outputLog(log: Log.Factory) = apply { this.outputLog = log }
    fun tableFilter(tableFilter: TableFilter) = apply { this.tableFilter = tableFilter }
}
