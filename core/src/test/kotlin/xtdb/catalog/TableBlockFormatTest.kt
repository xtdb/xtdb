package xtdb.catalog

import kotlinx.coroutines.flow.MutableStateFlow
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.api.TableRef
import xtdb.arrow.VectorType
import xtdb.block.proto.block
import xtdb.catalog.TableCatalog.Companion.buildTableBlock
import xtdb.catalog.TableCatalog.Companion.parseTableBlock
import xtdb.index.IndexCatalog
import xtdb.index.IndexDef
import xtdb.util.toHLL
import java.util.UUID

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

    @Test
    fun `a table only an index names gets an index-only block file, which loads as its indices and no table metadata`() {
        val bySku = IndexDef(UUID.randomUUID(), "by_sku", TableRef("public", "orders"), "sku")
        val indexCat = IndexCatalog.EMPTY.withIndex(bySku, 0, emptySet())

        val tableBlocks = TableCatalog(MutableStateFlow(TableCatalog.initialState(null)))
            .buildTableBlocks(emptyMap(), emptyMap(), indexCat)

        assertEquals(setOf(bySku.table), tableBlocks.keys)
        assertTrue(tableBlocks.getValue(bySku.table).indexOnly)

        assertNull(TableCatalog.loadState(block { blockIndex = 0 }, tableBlocks).rowCount(bySku.table))
        assertEquals(
            indexCat.indices.toList(),
            IndexCatalog.fromProto(tableBlocks.mapValues { (_, tableBlock) -> tableBlock.secondaryIndices }).indices.toList(),
        )
    }
}
