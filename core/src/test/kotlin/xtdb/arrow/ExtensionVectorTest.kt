package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.types.ClojureForm
import xtdb.types.ZonedDateTimeRange
import xtdb.util.Hasher
import xtdb.vector.ValueVectorReader.from
import xtdb.vector.extensions.KeywordType
import xtdb.vector.extensions.SetType
import xtdb.vector.extensions.TransitType
import xtdb.vector.extensions.TsTzRangeVector.Companion.tsTzRangeField
import xtdb.vector.extensions.UuidType
import xtdb.vector.writerFor
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.MICROS
import java.util.*

class ExtensionVectorTest {
    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    fun testEqualHashcode(vectorName: String, fieldType: FieldType, value: Any) {
        testEqualHashcode(Field(vectorName, fieldType, null), value)
    }

    fun testEqualHashcode(field: Field, value: Any) {
       field.createVector(al).use { oldVector ->
           Vector.fromField(al, field).use { newVector ->
               val writer = writerFor(oldVector)
               writer.writeObject(value)
               writer.asReader
               newVector.writeObject(value)

               val rdr = from(oldVector)
               val hasher = Hasher.Xx()

               assertEquals(rdr.hashCode(0, hasher), newVector.hashCode(0, hasher))
           }
       }
    }

    @Test
    fun `test extension vector hashCode for old and new`() {
        testEqualHashcode("keyword", FieldType.nullable(KeywordType), clojure.lang.Keyword.intern("foo"))
        testEqualHashcode("transit", FieldType.nullable(TransitType), ClojureForm(clojure.lang.Symbol.create("foo")))
        testEqualHashcode(Field("set", FieldType.nullable(SetType), listOf(Field("intChild", FieldType.nullable(MinorType.INT.type), null))), setOf(1, 2, 3))
        
        val zdt = ZonedDateTime.now(ZoneId.of("UTC")).truncatedTo(MICROS)
        testEqualHashcode(tsTzRangeField, ZonedDateTimeRange(zdt, zdt))

        testEqualHashcode("uuid", FieldType.nullable(UuidType), UUID.randomUUID())
    }

    @Test
    fun `test TimestampTzVector timezone compatibility issue`() {
        // This test demonstrates the issue found in the tstz-range investigation:
        // ZonedDateTime created with ZoneOffset.UTC vs ZoneId.of("UTC") are not equal
        // and cause InvalidWriteObjectException in TimestampTzVector.writeObject0

        val instant = java.time.Instant.now().truncatedTo(MICROS)

        // This creates a ZonedDateTime with ZoneOffset.UTC (what timestamp literals with 'Z' produce)
        val zonedWithOffset = instant.atZone(ZoneOffset.UTC)

        // This creates a ZonedDateTime with ZoneId.of("UTC") (what TimestampTzVector expects)
        val zonedWithZoneId = instant.atZone(ZoneId.of("UTC"))

        println("ZonedDateTime with ZoneOffset.UTC: $zonedWithOffset")
        println("  Zone: ${zonedWithOffset.zone} (${zonedWithOffset.zone::class.simpleName})")
        println("ZonedDateTime with ZoneId.of(\"UTC\"): $zonedWithZoneId")
        println("  Zone: ${zonedWithZoneId.zone} (${zonedWithZoneId.zone::class.simpleName})")
        println("Are zones equal? ${zonedWithOffset.zone == zonedWithZoneId.zone}")

        // Verify they represent the same time but have different zone types
        assertEquals(instant, zonedWithOffset.toInstant())
        assertEquals(instant, zonedWithZoneId.toInstant())
        assertNotEquals(zonedWithOffset.zone, zonedWithZoneId.zone, "Zones should NOT be equal")

        // Create a TimestampTzVector with ZoneId.of("UTC") (the default)
        TimestampTzVector(al, "test_ts", true, org.apache.arrow.vector.types.TimeUnit.MICROSECOND, ZoneId.of("UTC")).use { vector ->

            // This should work - zone matches
            vector.writeObject(zonedWithZoneId)
            assertEquals(zonedWithZoneId, vector.getObject(0))
            println("SUCCESS: ZonedDateTime with ZoneId.of(\"UTC\") worked")

            // With the fix, this should now work - zones should be considered equivalent
            vector.writeObject(zonedWithOffset)
            // Both timestamps should represent the same instant in time
            val retrievedZdt = vector.getObject(1)
            assertEquals(instant, (retrievedZdt as java.time.ZonedDateTime).toInstant())
            println("SUCCESS: ZonedDateTime with ZoneOffset.UTC now works with the fix!")
            println("Retrieved: $retrievedZdt")
            println("This confirms the fix resolves the tstz-range serialization issue")
        }
    }
}