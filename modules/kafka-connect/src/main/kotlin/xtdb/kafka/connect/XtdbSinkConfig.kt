package xtdb.kafka.connect

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.*
import org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE
import org.apache.kafka.common.config.ConfigDef.Type.STRING
import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigException

internal const val CONNECTION_URL_CONFIG: String = "connection.url"
internal const val ID_MODE_CONFIG: String = "id.mode"
internal const val TABLE_NAME_FORMAT_CONFIG: String = "table.name.format"

private class EnumValidator(private val validValues: Set<String>) : Validator {
    override fun ensureValid(key: String, value: Any?) {
        if (value != null && !validValues.contains(value)) {
            throw ConfigException(key, value, "Invalid enumerator")
        }
    }

    override fun toString(): String = validValues.toString()
}

internal val CONFIG_DEF: ConfigDef = ConfigDef()
    .define(
        CONNECTION_URL_CONFIG, STRING, NO_DEFAULT_VALUE, HIGH,
        "JDBC URL of XTDB server."
    )
    .define(
        ID_MODE_CONFIG, STRING, "record_value", EnumValidator(setOf("record_key", "record_value")), HIGH,
        "Where to get the `_id` from. Supported modes are `record_key` and `record_value`."
    )
    .define(
        TABLE_NAME_FORMAT_CONFIG, STRING, "\${topic}", MEDIUM,
        "A format string for the destination table name, which may contain `\${topic}` as a placeholder for the originating topic name."
    )

data class XtdbSinkConfig(
    val connectionUrl: String,
    var idMode: String,
    var tableNameFormat: String,
) {
    companion object {
        @JvmStatic
        fun parse(props: Map<String, String>): XtdbSinkConfig {
            val parsedConfig = AbstractConfig(CONFIG_DEF, props)
            val idMode = parsedConfig.getString(ID_MODE_CONFIG)

            return XtdbSinkConfig(
                connectionUrl = parsedConfig.getString(CONNECTION_URL_CONFIG),
                idMode = idMode,
                tableNameFormat = parsedConfig.getString(TABLE_NAME_FORMAT_CONFIG),
            )
        }
    }

    val taskConfig
        get() = mapOf(
            CONNECTION_URL_CONFIG to connectionUrl,
            ID_MODE_CONFIG to idMode,
            TABLE_NAME_FORMAT_CONFIG to tableNameFormat
        )

}