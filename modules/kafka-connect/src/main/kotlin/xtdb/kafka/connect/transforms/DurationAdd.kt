package xtdb.kafka.connect.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements.requireMap
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit


private const val TIMESTAMP_FIELD = "timestamp.field"
private const val DURATION_FIELD = "duration.field"
private const val DURATION_UNIT = "duration.unit"
private const val OUTPUT_FIELD = "output.field"
private const val PURPOSE = "to add the output field to."

@Suppress("unused")
class DurationAdd<R : ConnectRecord<R>> : Transformation<R> {

    companion object {
        private val CONFIG_DEF = ConfigDef().apply {
            define(
                TIMESTAMP_FIELD, Type.STRING, HIGH,
                "The field name to expect the timestamp in."
            )

            define(
                DURATION_FIELD, Type.STRING, HIGH,
                "The field name to expect the duration in."
            )

            define(
                DURATION_UNIT, Type.STRING, "S", MEDIUM,
                "The unit of the duration (from Java ChronoUnit). (Defaults to seconds)"
            )

            define(
                OUTPUT_FIELD, Type.STRING, HIGH,
                "The field name to output the result to."
            )
        }
    }

    private var timestampField: String? = null
    private var durationField: String? = null
    private var durationUnit: TemporalUnit = ChronoUnit.SECONDS
    private var outputField: String? = null

    override fun configure(configs: MutableMap<String, *>) {
        SimpleConfig(CONFIG_DEF, configs).run {
            timestampField = getString(TIMESTAMP_FIELD) ?: throw ConfigException(TIMESTAMP_FIELD, null)
            durationField = getString(DURATION_FIELD) ?: throw ConfigException(DURATION_FIELD, null)
            durationUnit = getString(DURATION_UNIT).let {
                try {
                    ChronoUnit.valueOf(it)
                } catch (_: Exception) {
                    throw ConfigException(DURATION_UNIT, it)
                }
            }

            outputField = getString(OUTPUT_FIELD) ?: throw ConfigException(OUTPUT_FIELD, null)
        }
    }

    override fun config() = CONFIG_DEF

    private fun newRecord(record: R, updatedValue: Map<String, *>) =
        record.newRecord(
            record.topic(), record.kafkaPartition(),
            record.keySchema(), record.key(),
            null, updatedValue,
            record.timestamp()
        )

    private fun applySchemaless(record: R): R {
        val value = requireMap(record.value(), PURPOSE).toMutableMap()

        val ts = when (val tsValue = value[timestampField]) {
            is Instant -> tsValue
            is String -> Instant.parse(tsValue)
            else -> throw DataException("""invalid timestamp: "$tsValue"""")
        }

        val duration = when (val durValue = value[durationField]) {
            is Long -> durValue
            is String -> durValue.toLong()
            else -> throw DataException("""invalid duration value: "$durValue"""")
        }

        value[outputField] = ts + Duration.of(duration, durationUnit)

        return newRecord(record, value)
    }

    override fun apply(record: R): R =
        if (record.valueSchema() == null) applySchemaless(record) else throw DataException("unexpected schema")

    override fun close() {
    }
}