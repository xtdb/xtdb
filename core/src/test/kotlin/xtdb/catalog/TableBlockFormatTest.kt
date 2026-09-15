package xtdb.catalog

import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorType
import xtdb.catalog.TableCatalog.Companion.buildTableBlock
import xtdb.catalog.TableCatalog.Companion.parseTableBlock
import xtdb.util.toHLL

class TableBlockFormatTest {

    private val vecTypes = mapOf(
        "_id" to VectorType.I64,
        "a" to VectorType.maybe(VectorType.UTF8),
        "l" to VectorType.Listy(ArrowType.List(), VectorType.I64),
        "s" to VectorType.Struct(mapOf("x" to VectorType.I64, "y" to VectorType.maybe(VectorType.UTF8)))
    )

    private val hlls = mapOf("_id" to toHLL(ByteArray(10) { it.toByte() }))

    private val block = buildTableBlock(vecTypes, rowCount = 3, partitions = emptyList(), hlls = hlls)

    @Test
    fun `a block recording a column tree is read through it`() {
        assertEquals(TableCatalog.TableMeta(vecTypes, 3, hlls), parseTableBlock(block))
    }

    @Test
    fun `a block recording no column tree is read through its arrow schema`() {
        assertEquals(parseTableBlock(block), parseTableBlock(block.toBuilder().clearColumns().build()))
    }

    @Test
    fun `a block recording a column tree does not consult its arrow schema`() {
        assertEquals(
            parseTableBlock(block),
            parseTableBlock(block.toBuilder().clearArrowSchema().clearColumnNameToHll().build())
        )
    }
}
