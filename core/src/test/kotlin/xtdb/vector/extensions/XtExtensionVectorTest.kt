package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.types.ClojureForm
import xtdb.types.ZonedDateTimeRange
import xtdb.util.Hasher
import xtdb.vector.ValueVectorReader.from
import xtdb.vector.writerFor
import java.time.ZonedDateTime
import java.util.*

class XtExtensionVectorTest {
    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    @Test
    fun `test ExtensionVector TransferPair` () {
        val uuidField = Field("uuid", FieldType.notNullable(UuidType), emptyList())
        UuidType.getNewVector("uuid", FieldType.nullable(UuidType), al).use {  uuidVector ->
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            uuidVector.setObject(0, uuid1)
            uuidVector.setObject(1, uuid2)
            uuidVector.valueCount = 2

            val newVector = uuidVector
                .getTransferPair(uuidField, al)
                .also { it.transfer() }
                .to as FieldVector

            assertEquals(uuidField, newVector.field)
            assertEquals(2, newVector.valueCount)
            assertEquals(listOf(uuid1, uuid2), from(newVector).toList())

            newVector.close()
        }
    }

    @Test
    fun `test correct fieldType` () {
        assertThrows<IllegalArgumentException> { UuidVector("uuid", al, FieldType.nullable(MinorType.INT.type)) }
        assertThrows<IllegalArgumentException> { TransitVector("transit", al, FieldType.nullable(MinorType.INT.type)) }
    }

    @Test
    fun `test ExtensionVector hashCode` () {
        UuidVector("uuid", al, FieldType.notNullable(UuidType)).use {  uuidVector ->
            uuidVector.setObject(0, UUID.randomUUID())
            uuidVector.valueCount = 1

            val rdr = from(uuidVector)
            val hasher = Hasher.Xx()
            rdr.hashCode(0, hasher)
        }

        TransitVector("transit", al, FieldType.notNullable(TransitType)).use {  transitVector ->
            transitVector.setObject(0, ClojureForm(clojure.lang.Symbol.create("foo")))
            transitVector.valueCount = 1

            val rdr = from(transitVector)
            val hasher = Hasher.Xx()
            rdr.hashCode(0, hasher)

        }

        TsTzRangeVector("tstzrange", al, FieldType.notNullable(TsTzRangeType)).use {  tstzrangeVector ->
            tstzrangeVector.setObject(0, ZonedDateTimeRange(ZonedDateTime.now(), ZonedDateTime.now()))
            tstzrangeVector.valueCount = 1

            val rdr = from(tstzrangeVector)
            val hasher = Hasher.Xx()
            rdr.hashCode(0, hasher)
        }
    }
}