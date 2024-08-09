package xtdb.kafka.connect

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.*
import org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE
import org.apache.kafka.common.config.ConfigDef.Type.STRING
import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigException

internal const val URL_CONFIG: String = "url"
internal const val ID_MODE_CONFIG: String = "id.mode"
internal const val ID_FIELD_CONFIG: String = "id.field"
internal const val VALID_FROM_FIELD_CONFIG: String = "validFrom.field"
internal const val VALID_TO_FIELD_CONFIG: String = "validTo.field"
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
        URL_CONFIG, STRING, NO_DEFAULT_VALUE, HIGH,
        "Destination URL of XTDB HTTP end point."
    )
    .define(
        ID_MODE_CONFIG, STRING, NO_DEFAULT_VALUE, EnumValidator(setOf("record_key", "record_value")), HIGH,
        "Where to get the `_id` from. Supported modes are `record_key` and `record_value`."
    )
    .define(
        ID_FIELD_CONFIG, STRING, "", MEDIUM,
        "The field name to use as the `_id`. Leave blank if using a primitive `record_key`."
    )
    .define(
        VALID_FROM_FIELD_CONFIG, STRING, "", LOW,
        "The field name to use as `_valid_from`. Leave blank to use XTDB's default `_valid_from`."
    )
    .define(
        VALID_TO_FIELD_CONFIG, STRING, "", LOW,
        "The field name to use as `_valid_to`. Leave blank to use XTDB's default `_valid_to`."
    )
    .define(
        TABLE_NAME_FORMAT_CONFIG, STRING, "\${topic}", MEDIUM,
        "A format string for the destination table name, which may contain `\${topic}` as a placeholder for the originating topic name."
    )

data class XtdbSinkConfig(
    val url: String,
    var idMode: String,
    var idField: String,
    var validFromField: String,
    var validToField: String,
    var tableNameFormat: String,
) {
    companion object {
        internal fun parse(props: Map<String, String>): XtdbSinkConfig {
            val parsedConfig = AbstractConfig(CONFIG_DEF, props)
            val idMode = parsedConfig.getString(ID_MODE_CONFIG)
            val idField = parsedConfig.getString(ID_FIELD_CONFIG)

            if (idMode == "record_value" && (idField ?: "").isEmpty()) {
                throw ConfigException(ID_FIELD_CONFIG, idField, "id.field must be set when id.mode is record_value")
            }

            return XtdbSinkConfig(
                url = parsedConfig.getString(URL_CONFIG),
                idMode = idMode,
                idField = idField,
                validFromField = parsedConfig.getString(VALID_FROM_FIELD_CONFIG),
                validToField = parsedConfig.getString(VALID_TO_FIELD_CONFIG),
                tableNameFormat = parsedConfig.getString(TABLE_NAME_FORMAT_CONFIG),
            )
        }
    }

    val taskConfig
        get() = mapOf(
            URL_CONFIG to url,
            ID_MODE_CONFIG to idMode,
            ID_FIELD_CONFIG to idField,
            VALID_FROM_FIELD_CONFIG to validFromField,
            VALID_TO_FIELD_CONFIG to validToField,
            TABLE_NAME_FORMAT_CONFIG to tableNameFormat
        )

}