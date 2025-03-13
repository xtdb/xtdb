package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.types.ClojureForm
import xtdb.types.ZonedDateTimeRange
import xtdb.util.Hasher
import xtdb.vector.ValueVectorReader.from
import xtdb.vector.extensions.*
import xtdb.vector.extensions.TsTzRangeVector
import xtdb.vector.writerFor
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.*
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
               writer.syncValueCount()
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
        testEqualHashcode(TsTzRangeVector.Companion.tsTzRangeField, ZonedDateTimeRange(zdt, zdt))

        testEqualHashcode("uuid", FieldType.nullable(UuidType), UUID.randomUUID())
    }
}