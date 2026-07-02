package xtdb.kafka.connectsrc

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.storage.Converter
import org.apache.kafka.connect.storage.ConverterConfig
import org.apache.kafka.connect.storage.ConverterType
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.predicates.Predicate
import xtdb.error.Incorrect
import xtdb.util.safelyOpening

internal enum class ConverterRole(val configKey: String, val type: ConverterType) {
    KEY("key.converter", ConverterType.KEY),
    VALUE("value.converter", ConverterType.VALUE);

    val isKey get() = type == ConverterType.KEY
}

/**
 * A parsed view of a Kafka Connect source's `connectConfig`, resolved once at [KafkaConnectSource.Factory.open]
 * and turned into live converter/transform/predicate instances when the partition is assigned.
 *
 * The key space is closed: `connectConfig` holds converter/transform/predicate options only, so a typo'd or
 * out-of-place key (e.g. a Kafka client setting, which belongs on the `!Kafka` remote) fails parse rather
 * than silently doing nothing.
 */
internal data class ConnectConfig(
    val keyConverter: ConverterSpec,
    val valueConverter: ConverterSpec,
    val transforms: List<TransformSpec>,
    val predicates: Map<String, PredicateSpec>,
) {
    internal data class ConverterSpec(val role: ConverterRole, val className: String, val config: Map<String, String>) {
        fun open(): Converter =
            loadPlugin<Converter>(className, role.configKey)
                .also {
                    // `converter.type` is required by ConverterConfig but injected by configure(_, isKey), so
                    // validation must inject it too — as KC's AbstractHerder.validateConverterConfig does.
                    val props = buildMap {
                        putAll(config)
                        // getName() is KC's lowercase "key"/"value" — Kotlin's `.name` would resolve to
                        // Enum.name ("KEY") and fail ConverterConfig's `in(...)` validator
                        putIfAbsent(ConverterConfig.TYPE_CONFIG, role.type.getName())
                    }
                    it.config()?.validateOrThrow(props, role.configKey)
                }
                .apply { configure(config, role.isKey) }
    }

    internal data class PredicateSpec(val alias: String, val className: String, val config: Map<String, String>) {
        fun open(): Predicate<SinkRecord> =
            loadPlugin<Predicate<SinkRecord>>(className, "predicate")
                .also { it.config()?.validateOrThrow(config, "predicates.$alias") }
                .apply { configure(config) }
    }

    internal data class TransformSpec(
        val alias: String,
        val className: String,
        val config: Map<String, String>,
        val predicateAlias: String?,
        val negate: Boolean,
    ) {
        fun open(): Transformation<SinkRecord> =
            loadPlugin<Transformation<SinkRecord>>(className, "transform")
                .also { it.config()?.validateOrThrow(config, "transforms.$alias") }
                .apply { configure(config) }
    }

    /**
     * Instantiates the transforms and the predicates they reference into a runnable [TransformChain].
     * A predicate is opened once per alias, so one shared by several transforms is a single instance
     * (as in Kafka Connect). If a later [open] throws, everything already opened is closed — the chain
     * is all-or-nothing, so a half-built chain never leaks configured instances the caller can't close.
     */
    fun openTransformChain(): TransformChain = safelyOpening {
        val referenced = transforms.mapNotNull { it.predicateAlias }.toSet()
        val openPredicates = referenced.associateWith { alias -> open { predicates.getValue(alias).open() } }
        val steps = transforms.map { t ->
            TransformChain.Step(open { t.open() }, t.predicateAlias?.let(openPredicates::getValue), t.negate)
        }
        TransformChain(steps, openPredicates.values)
    }

    companion object {
        fun parse(connectConfig: Map<String, String>): ConnectConfig {
            fun converter(role: ConverterRole): ConverterSpec {
                val key = role.configKey
                val className = connectConfig[key]
                    ?: throw Incorrect(
                        "missing '$key' in connectConfig",
                        "xtdb.kafka-connect-source/missing-converter",
                        mapOf("role" to key),
                    )
                return ConverterSpec(role, className, connectConfig.subConfig("$key."))
            }

            fun predicateSpec(alias: String): PredicateSpec {
                val className = connectConfig["predicates.$alias.type"]
                    ?: throw Incorrect(
                        "missing 'predicates.$alias.type'",
                        "xtdb.kafka-connect-source/missing-predicate-type",
                        mapOf("alias" to alias),
                    )
                return PredicateSpec(alias, className, connectConfig.subConfig("predicates.$alias.", reserved = setOf("type")))
            }

            val transformAliases = aliasList(connectConfig["transforms"], "transforms")
            val predicateAliases = aliasList(connectConfig["predicates"], "predicates")

            for (alias in transformAliases) {
                val ref = connectConfig["transforms.$alias.predicate"] ?: continue
                if (ref !in predicateAliases) throw Incorrect(
                    "transform '$alias' references predicate '$ref', which isn't declared in 'predicates'",
                    "xtdb.kafka-connect-source/undeclared-predicate",
                    mapOf("alias" to alias, "predicate" to ref),
                )
            }

            for (key in connectConfig.keys) {
                val recognised = key == "key.converter" || key == "value.converter"
                    || key.startsWith("key.converter.") || key.startsWith("value.converter.")
                    || key == "transforms" || key == "predicates"
                    || transformAliases.any { key.startsWith("transforms.$it.") }
                    || predicateAliases.any { key.startsWith("predicates.$it.") }

                if (!recognised) throw Incorrect(
                    "unknown connectConfig key '$key' — connectConfig accepts converter, transform, and predicate " +
                        "options only (Kafka client settings go on the !Kafka remote; other Kafka Connect " +
                        "worker/connector settings aren't applicable here)",
                    "xtdb.kafka-connect-source/unknown-connect-config-key",
                    mapOf("key" to key),
                )
            }

            val transforms = transformAliases.map { alias ->
                val typeKey = "transforms.$alias.type"
                val type = connectConfig[typeKey]
                    ?: throw Incorrect(
                        "missing '$typeKey' for transform alias '$alias'",
                        "xtdb.kafka-connect-source/missing-transform-type",
                        mapOf("alias" to alias),
                    )
                val predicateAlias = connectConfig["transforms.$alias.predicate"]
                val negate = connectConfig["transforms.$alias.negate"]?.let { raw ->
                    if (predicateAlias == null) throw Incorrect(
                        "'transforms.$alias.negate' without 'transforms.$alias.predicate' — negate inverts a predicate",
                        "xtdb.kafka-connect-source/negate-without-predicate",
                        mapOf("alias" to alias),
                    )
                    // trim + case-insensitive, matching KC's ConfigDef BOOLEAN parsing
                    raw.trim().lowercase().toBooleanStrictOrNull() ?: throw Incorrect(
                        "invalid 'transforms.$alias.negate' value '$raw' — expected 'true' or 'false'",
                        "xtdb.kafka-connect-source/invalid-negate",
                        mapOf("alias" to alias, "value" to raw),
                    )
                } == true
                val config = connectConfig
                    .subConfig("transforms.$alias.", reserved = setOf("type", "predicate", "negate"))

                TransformSpec(alias, type, config, predicateAlias, negate)
            }

            val predicates = predicateAliases.associateWith { predicateSpec(it) }

            return ConnectConfig(converter(ConverterRole.KEY), converter(ConverterRole.VALUE), transforms, predicates)
        }
    }
}

/**
 * Runs a plugin's own ConfigDef validation, as Kafka Connect does — so a plugin that declares its rules
 * via `config()` rather than checking in `configure()` still has its options checked. Undeclared keys
 * aren't flagged (ConfigDef.validate ignores them).
 */
private fun ConfigDef.validateOrThrow(props: Map<String, String>, what: String) {
    val errors = validate(props).flatMap { cv -> cv.errorMessages().map { "${cv.name()}: $it" } }
    if (errors.isNotEmpty()) throw Incorrect(
        "invalid config for $what: ${errors.joinToString("; ")}",
        "xtdb.kafka-connect-source/invalid-plugin-config",
        mapOf("what" to what, "errors" to errors),
    )
}

/**
 * Loads [className] and instantiates it via its no-arg constructor, checking it really is a [T].
 * [role] labels the plugin (e.g. `key.converter`, `transform`) in the error if the load fails.
 *
 * NOTE: different from KC. KC's `Plugins` loads each plugin from an isolated classpath for
 * dependency isolation between plugins; we load from the node's own classpath.
 */
private inline fun <reified T : Any> loadPlugin(className: String, role: String): T {
    val cls = try {
        Class.forName(className).asSubclass(T::class.java)
    } catch (e: Exception) {
        throw Incorrect(
            "couldn't load $role class '$className': ${e.message}",
            "xtdb.kafka-connect-source/plugin-class-not-found",
            mapOf("role" to role, "className" to className),
            cause = e,
        )
    }
    return cls.getDeclaredConstructor().newInstance()
}

/** Parses a comma-separated Kafka Connect alias list (`transforms`/`predicates`), rejecting empty and duplicate aliases. */
private fun aliasList(raw: String?, key: String): List<String> {
    val trimmed = raw?.trim().orEmpty()
    if (trimmed.isEmpty()) return emptyList()
    val aliases = trimmed.split(",").map { it.trim() }
    if (aliases.any { it.isEmpty() }) throw Incorrect(
        "empty alias in '$key' — check for stray commas",
        "xtdb.kafka-connect-source/empty-alias",
        mapOf("key" to key, "aliases" to trimmed),
    )
    val duplicates = aliases.groupingBy { it }.eachCount().filterValues { it > 1 }.keys
    if (duplicates.isNotEmpty()) throw Incorrect(
        "duplicate alias '${duplicates.first()}' in '$key'",
        "xtdb.kafka-connect-source/duplicate-alias",
        mapOf("key" to key, "duplicates" to duplicates.toList()),
    )
    return aliases
}

/**
 * The options under [prefix], with [prefix] stripped, dropping the [reserved] framework keys.
 *
 * [reserved] matches the stripped key exactly, so a plugin option that merely ends in a reserved
 * name — e.g. `TimestampConverter`'s `target.type` under a `transforms.<alias>.` prefix — is kept.
 */
private fun Map<String, String>.subConfig(prefix: String, reserved: Set<String> = emptySet()): Map<String, String> =
    filterKeys { it.startsWith(prefix) }
        .mapKeys { (k, _) -> k.removePrefix(prefix) }
        .filterKeys { it !in reserved }
