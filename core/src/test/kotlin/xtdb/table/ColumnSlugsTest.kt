package xtdb.table

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorType

class ColumnSlugsTest {

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
}
