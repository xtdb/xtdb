package xtdb.table

import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorType
import xtdb.arrow.extensions.SetType
import xtdb.util.toHLL

class ColumnMetaTest {

    private val vocabulary = listOf(
        VectorType.Nothing, VectorType.Null,

        VectorType.BOOL, VectorType.I8, VectorType.I16, VectorType.I32, VectorType.I64,
        VectorType.F32, VectorType.F64, VectorType.UTF8, VectorType.VAR_BINARY,
        VectorType.KEYWORD, VectorType.TRANSIT, VectorType.URI, VectorType.UUID,
        VectorType.OID, VectorType.REG_CLASS, VectorType.REG_PROC,

        VectorType.IID,
        VectorType.Scalar(ArrowType.Decimal(38, 10, 128)),

        VectorType.INSTANT, VectorType.TIMESTAMP_MICRO,
        VectorType.Scalar(ArrowType.Timestamp(TimeUnit.SECOND, null)),
        VectorType.Scalar(ArrowType.Timestamp(TimeUnit.MILLISECOND, "Europe/London")),
        VectorType.Scalar(ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),

        VectorType.DATE_DAY, VectorType.Scalar(ArrowType.Date(DateUnit.MILLISECOND)),
        VectorType.TIME_MICRO, VectorType.TIME_NANO, VectorType.Scalar(ArrowType.Time(TimeUnit.SECOND, 32)),
        VectorType.DURATION_MICRO, VectorType.Scalar(ArrowType.Duration(TimeUnit.NANOSECOND)),

        VectorType.INTERVAL_YEAR, VectorType.INTERVAL_MDN, VectorType.INTERVAL_MDM,
        VectorType.Scalar(ArrowType.Interval(IntervalUnit.DAY_TIME)),

        VectorType.TSTZ_RANGE,
        VectorType.Listy(ArrowType.List(), VectorType.I64),
        VectorType.Listy(SetType, VectorType.UTF8),
        VectorType.Listy(ArrowType.FixedSizeList(3), VectorType.F64),
        VectorType.Listy(ArrowType.Map(true), VectorType.Struct(mapOf("key" to VectorType.UTF8, "value" to VectorType.I64))),

        VectorType.Struct(mapOf("a" to VectorType.I64, "b" to VectorType.UTF8)),
        VectorType.maybe(VectorType.I64),
        VectorType.fromLegs(VectorType.I64, VectorType.UTF8),
        VectorType.fromLegs(VectorType.I64, VectorType.UTF8, VectorType.Null),

        VectorType.Struct(
            mapOf(
                "nested" to VectorType.maybe(
                    VectorType.Struct(mapOf("deep" to VectorType.fromLegs(VectorType.I64, VectorType.UTF8)))
                )
            )
        )
    )

    @Test
    fun `every type in the vocabulary survives the proto round trip`() {
        assertEquals(
            vocabulary,
            vocabulary.map { ColumnMeta.fromProto("c", ColumnMeta.of("c", it).toProto()).type.vectorType }
        )
    }

    @Test
    fun `a column recorded before names existed takes its slug as its name, at every depth`() {
        val proto = ColumnMeta.of("s", VectorType.Struct(mapOf("z" to VectorType.I64))).toProto()

        val nameless = proto.toBuilder().apply {
            clearName()
            type = type.toBuilder().apply {
                struct = struct.toBuilder()
                    .putChildren("z", struct.getChildrenOrThrow("z").toBuilder().clearName().build())
                    .build()
            }.build()
        }.build()

        val col = ColumnMeta.fromProto("s", nameless)

        assertEquals("s", col.name)
        assertEquals("z", (col.type as ColumnMeta.Type.Struct).children.getValue("z").name)
    }

    @Test
    fun `a name survives the round trip where it differs from the slug`() {
        val renamed = ColumnMeta.of("s", VectorType.I64).copy(name = "renamed")

        assertEquals("renamed", ColumnMeta.fromProto("s", renamed.toProto()).name)
    }

    @Test
    fun `an hll survives the round trip, and its absence does too`() {
        val hll = toHLL(ByteArray(10) { it.toByte() })

        assertEquals(hll, ColumnMeta.fromProto("c", ColumnMeta.of("c", VectorType.I64, hll).toProto()).hll)
        assertNull(ColumnMeta.fromProto("c", ColumnMeta.of("c", VectorType.I64).toProto()).hll)
    }
}
