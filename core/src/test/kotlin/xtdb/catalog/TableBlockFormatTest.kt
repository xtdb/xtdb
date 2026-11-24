package xtdb.catalog

import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorType
import xtdb.catalog.TableCatalog.Companion.buildTableBlock
import xtdb.catalog.TableCatalog.Companion.mergeTables
import xtdb.catalog.TableCatalog.Companion.parseTableBlock
import xtdb.table.PrimaryKey
import xtdb.util.toHLL

class TableBlockFormatTest {

    private val vecTypes = mapOf(
        "_id" to VectorType.I64,
        "a" to VectorType.maybe(VectorType.UTF8),
        "l" to VectorType.Listy(ArrowType.List(), VectorType.I64),
        "s" to VectorType.Struct(mapOf("x" to VectorType.I64, "y" to VectorType.maybe(VectorType.UTF8)))
    )

    private val hlls = mapOf("_id" to toHLL(ByteArray(10) { it.toByte() }))

    private val block = buildTableBlock(vecTypes, rowCount = 3, partitions = emptyList(), hlls = hlls, primaryKey = null)

    @Test
    fun `a block recording a column tree is read through it`() {
        assertEquals(TableCatalog.TableMeta(vecTypes, 3, hlls, PrimaryKey.ID), parseTableBlock(block))
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
    fun `a block records its table's primary key`() {
        val pk = PrimaryKey(listOf("a", "_id"))

        assertEquals(
            pk,
            parseTableBlock(buildTableBlock(vecTypes, rowCount = 3, partitions = emptyList(), hlls = hlls, primaryKey = pk))
                .primaryKey
        )
    }

    @Test
    fun `a block recording no primary key is keyed on _id`() {
        assertEquals(PrimaryKey.ID, parseTableBlock(block.toBuilder().clearPrimaryKey().build()).primaryKey)
    }

    @Test
    fun `a table's primary key is carried into its next block`() {
        val pk = PrimaryKey(listOf("a"))
        val stored = parseTableBlock(buildTableBlock(vecTypes, rowCount = 3, partitions = emptyList(), hlls = hlls, primaryKey = pk))
        val next = mergeTables(stored, TableCatalog.TableMeta(emptyMap(), 2, emptyMap(), primaryKey = null))

        assertEquals(
            pk,
            parseTableBlock(buildTableBlock(next.vecTypes, next.rowCount, emptyList(), next.hlls, next.primaryKey)).primaryKey
        )
    }
}
