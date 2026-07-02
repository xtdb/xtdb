package xtdb.kafka.connectsrc

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.predicates.Predicate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.error.Incorrect
import java.util.concurrent.atomic.AtomicInteger

/** Records how many times its instances are closed, so a test can assert the chain rolled back. */
class RecordingTransform : Transformation<SinkRecord> {
    override fun configure(configs: MutableMap<String, *>?) {}
    override fun apply(record: SinkRecord): SinkRecord = record
    override fun config(): ConfigDef = ConfigDef()
    override fun close() { closeCount.incrementAndGet() }

    companion object {
        val closeCount = AtomicInteger(0)
    }
}

/** Counts instances opened and closed, so a test can assert a shared predicate is a single instance. */
class RecordingPredicate : Predicate<SinkRecord> {
    init { openCount.incrementAndGet() }
    override fun configure(configs: MutableMap<String, *>?) {}
    override fun config(): ConfigDef = ConfigDef()
    override fun test(record: SinkRecord): Boolean = true
    override fun close() { closeCount.incrementAndGet() }

    companion object {
        val openCount = AtomicInteger(0)
        val closeCount = AtomicInteger(0)
    }
}

/** Declares its rules only via [config] — the plugin style that relies on the caller running validation. */
class ValidatedTransform : Transformation<SinkRecord> {
    override fun configure(configs: MutableMap<String, *>?) {}
    override fun apply(record: SinkRecord): SinkRecord = record
    override fun config(): ConfigDef =
        ConfigDef().define("required.opt", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "a required option")
    override fun close() {}
}

class ValidatedPredicate : Predicate<SinkRecord> {
    override fun configure(configs: MutableMap<String, *>?) {}
    override fun test(record: SinkRecord): Boolean = true
    override fun config(): ConfigDef =
        ConfigDef().define("required.opt", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "a required option")
    override fun close() {}
}

class ConnectConfigTest {

    // parse does no class loading, so the class names here need not resolve.
    private val converters = mapOf(
        "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
        "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
    )

    @Test
    fun `parses converters with their nested config`() {
        val cfg = ConnectConfig.parse(converters + mapOf("value.converter.schemas.enable" to "false"))

        assertEquals("org.apache.kafka.connect.storage.StringConverter", cfg.keyConverter.className)
        assertEquals("org.apache.kafka.connect.json.JsonConverter", cfg.valueConverter.className)
        assertEquals(mapOf("schemas.enable" to "false"), cfg.valueConverter.config)
        assertTrue(cfg.transforms.isEmpty())
    }

    @Test
    fun `requires key and value converters`() {
        assertThrows<Incorrect> { ConnectConfig.parse(mapOf("value.converter" to "x")) }
        assertThrows<Incorrect> { ConnectConfig.parse(mapOf("key.converter" to "x")) }
    }

    @Test
    fun `parses a transform with its config and a negated predicate`() {
        val cfg = ConnectConfig.parse(
            converters + mapOf(
                "transforms" to "mask",
                "transforms.mask.type" to "com.example.Mask",
                "transforms.mask.fields" to "email",
                "transforms.mask.predicate" to "p",
                "transforms.mask.negate" to "true",
                "predicates" to "p",
                "predicates.p.type" to "com.example.NotNull",
            )
        )

        val t = cfg.transforms.single()
        assertEquals("mask", t.alias)
        assertEquals("com.example.Mask", t.className)
        assertEquals(mapOf("fields" to "email"), t.config)
        assertTrue(t.negate)
        assertEquals("p", t.predicateAlias)
    }

    @Test
    fun `parses negate the way ConfigDef parses booleans — trimmed, any case`() {
        fun negate(value: String) = ConnectConfig.parse(
            converters + mapOf(
                "transforms" to "mask",
                "transforms.mask.type" to "com.example.Mask",
                "transforms.mask.predicate" to "p",
                "transforms.mask.negate" to value,
                "predicates" to "p",
                "predicates.p.type" to "com.example.NotNull",
            )
        ).transforms.single().negate

        assertTrue(negate("True"))
        assertTrue(negate(" TRUE "))
        assertEquals(false, negate("False"))
    }

    @Test
    fun `keeps plugin options whose names end in a reserved key`() {
        val cfg = ConnectConfig.parse(
            converters + mapOf(
                "transforms" to "ts",
                "transforms.ts.type" to "org.apache.kafka.connect.transforms.TimestampConverter",
                "transforms.ts.field" to "event_time",
                "transforms.ts.target.type" to "Timestamp",
            )
        )

        assertEquals(
            mapOf("field" to "event_time", "target.type" to "Timestamp"),
            cfg.transforms.single().config,
        )
    }

    @Test
    fun `requires a referenced predicate to be declared in the predicates list`() {
        assertThrows<Incorrect> {
            ConnectConfig.parse(
                converters + mapOf(
                    "transforms" to "mask",
                    "transforms.mask.type" to "com.example.Mask",
                    "transforms.mask.predicate" to "notTombstone",
                    "predicates.notTombstone.type" to "com.example.NotTombstone",
                )
            )
        }
    }

    @Test
    fun `reads the top-level predicates list with its config`() {
        val cfg = ConnectConfig.parse(
            converters + mapOf(
                "transforms" to "mask",
                "transforms.mask.type" to "com.example.Mask",
                "transforms.mask.predicate" to "p",
                "predicates" to "p",
                "predicates.p.type" to "com.example.NotNull",
                "predicates.p.field" to "email",
            )
        )

        assertEquals("com.example.NotNull", cfg.predicates.getValue("p").className)
        assertEquals(mapOf("field" to "email"), cfg.predicates.getValue("p").config)
    }

    @Test
    fun `requires a type for each transform`() {
        assertThrows<Incorrect> { ConnectConfig.parse(converters + mapOf("transforms" to "mask")) }
    }

    @Test
    fun `parses a declared predicate no transform references`() {
        val cfg = ConnectConfig.parse(
            converters + mapOf(
                "predicates" to "p",
                "predicates.p.type" to "com.example.NotNull",
            )
        )

        assertEquals("com.example.NotNull", cfg.predicates.getValue("p").className)
    }

    @Test
    fun `requires a type for a declared predicate`() {
        assertThrows<Incorrect> { ConnectConfig.parse(converters + mapOf("predicates" to "p")) }
    }

    @Test
    fun `rejects an empty predicate alias`() {
        assertThrows<Incorrect> {
            ConnectConfig.parse(
                converters + mapOf("predicates" to "a,,b", "predicates.a.type" to "x", "predicates.b.type" to "y")
            )
        }
    }

    @Test
    fun `rejects negate without a predicate`() {
        assertThrows<Incorrect> {
            ConnectConfig.parse(
                converters + mapOf(
                    "transforms" to "mask",
                    "transforms.mask.type" to "com.example.Mask",
                    "transforms.mask.negate" to "true",
                )
            )
        }
    }

    @Test
    fun `rejects a negate value that isn't a boolean`() {
        assertThrows<Incorrect> {
            ConnectConfig.parse(
                converters + mapOf(
                    "transforms" to "mask",
                    "transforms.mask.type" to "com.example.Mask",
                    "transforms.mask.predicate" to "p",
                    "transforms.mask.negate" to "yes",
                    "predicates" to "p",
                    "predicates.p.type" to "com.example.NotNull",
                )
            )
        }
    }

    @Test
    fun `rejects duplicate transform aliases`() {
        assertThrows<Incorrect> {
            ConnectConfig.parse(converters + mapOf("transforms" to "mask,mask", "transforms.mask.type" to "x"))
        }
    }

    @Test
    fun `rejects duplicate predicate aliases`() {
        assertThrows<Incorrect> {
            ConnectConfig.parse(converters + mapOf("predicates" to "p,p", "predicates.p.type" to "x"))
        }
    }

    @Test
    fun `rejects an unrecognised key such as a typo`() {
        assertThrows<Incorrect> { ConnectConfig.parse(converters + mapOf("transfroms" to "mask")) }
    }

    @Test
    fun `rejects a Kafka client key — tuning belongs on the remote`() {
        assertThrows<Incorrect> { ConnectConfig.parse(converters + mapOf("max.poll.records" to "100")) }
    }

    @Test
    fun `rejects options under an undeclared transform alias`() {
        assertThrows<Incorrect> {
            ConnectConfig.parse(
                converters + mapOf(
                    "transforms" to "mask",
                    "transforms.mask.type" to "com.example.Mask",
                    "transforms.msak.fields" to "email",
                )
            )
        }
    }

    @Test
    fun `rejects an empty transform alias`() {
        assertThrows<Incorrect> {
            ConnectConfig.parse(
                converters + mapOf("transforms" to "a,,b", "transforms.a.type" to "x", "transforms.b.type" to "y")
            )
        }
    }

    @Test
    fun `reports a friendly error when a transform class can't be loaded`() {
        assertThrows<Incorrect> {
            ConnectConfig.TransformSpec("t", "com.example.DoesNotExist", emptyMap(), null, false).open()
        }
    }

    @Test
    fun `reports a friendly error when a transform class isn't a Transformation`() {
        assertThrows<Incorrect> {
            ConnectConfig.TransformSpec("t", "java.lang.String", emptyMap(), null, false).open()
        }
    }

    @Test
    fun `reports a friendly error when a predicate class isn't a Predicate`() {
        assertThrows<Incorrect> {
            ConnectConfig.PredicateSpec("p", "java.lang.String", emptyMap()).open()
        }
    }

    @Test
    fun `validates a converter's options, injecting the converter type`() {
        val spec = ConnectConfig.ConverterSpec(
            ConverterRole.VALUE,
            "org.apache.kafka.connect.json.JsonConverter",
            mapOf("schemas.cache.size" to "not-a-number"),
        )
        assertThrows<Incorrect> { spec.open() }

        // a valid config opens — validation doesn't trip over `converter.type`, which only configure() injects
        spec.copy(config = mapOf("schemas.enable" to "false")).open().use {}
    }

    @Test
    fun `validates a transform's options against its ConfigDef`() {
        val spec = ConnectConfig.TransformSpec("t", ValidatedTransform::class.java.name, emptyMap(), null, false)
        assertThrows<Incorrect> { spec.open() }

        spec.copy(config = mapOf("required.opt" to "x")).open().use {}
    }

    @Test
    fun `validates a predicate's options against its ConfigDef`() {
        val spec = ConnectConfig.PredicateSpec("p", ValidatedPredicate::class.java.name, emptyMap())
        assertThrows<Incorrect> { spec.open() }

        spec.copy(config = mapOf("required.opt" to "x")).open().use {}
    }

    @Test
    fun `closes already-opened transforms when a later one fails to open`() {
        RecordingTransform.closeCount.set(0)
        val cfg = ConnectConfig.parse(
            converters + mapOf(
                "transforms" to "ok,bad",
                "transforms.ok.type" to "xtdb.kafka.connectsrc.RecordingTransform",
                "transforms.bad.type" to "com.example.DoesNotExist",
            )
        )

        assertThrows<Incorrect> { cfg.openTransformChain() }
        assertEquals(1, RecordingTransform.closeCount.get())
    }

    @Test
    fun `opens a shared predicate once and closes it once`() {
        RecordingPredicate.openCount.set(0)
        RecordingPredicate.closeCount.set(0)
        val cfg = ConnectConfig.parse(
            converters + mapOf(
                "transforms" to "a,b",
                "transforms.a.type" to "xtdb.kafka.connectsrc.RecordingTransform",
                "transforms.a.predicate" to "p",
                "transforms.b.type" to "xtdb.kafka.connectsrc.RecordingTransform",
                "transforms.b.predicate" to "p",
                "predicates" to "p",
                "predicates.p.type" to "xtdb.kafka.connectsrc.RecordingPredicate",
            )
        )

        cfg.openTransformChain().use {
            assertEquals(1, RecordingPredicate.openCount.get())
        }
        assertEquals(1, RecordingPredicate.closeCount.get())
    }
}
