package xtdb.catalog

import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.TableRef
import xtdb.storage.MemoryStorage

class DatabaseTableCatalogTest {

    private lateinit var allocator: RootAllocator

    @BeforeEach
    fun setUp() { allocator = RootAllocator() }

    @AfterEach
    fun tearDown() { allocator.close() }

    private val foo = TableRef("public", "foo")
    private val bar = TableRef("public", "bar")

    private fun tableCatalog(vararg tables: Pair<TableRef, List<String>>) =
        TableCatalog(MemoryStorage(allocator, epoch = 0))
            .also { cat -> tables.forEach { (table, cols) -> cat.seedTable(table, cols) } }

    @Test
    fun `getType agrees with getTypes for a column only one partition has`() {
        val cat = DatabaseTableCatalog(
            listOf(
                tableCatalog(foo to listOf("_id", "v")),
                tableCatalog(foo to listOf("_id")),
            )
        )

        // the whole point: partition 1 has the table but not the column, so it has to contribute an
        // absent `v` to the merge rather than drop out of it. Reducing each partition's `getType`
        // can't see the difference between "no table here" and "no such column here".
        assertEquals(
            cat.getTypes(foo)?.get("v"), cat.getType(foo, "v"),
            "getType and getTypes agree about the same column"
        )

        assertNull(cat.getType(foo, "no_such_col"), "a column no partition has is absent, not Null")
        assertNull(cat.getTypes(bar), "a table no partition has is absent, not empty")
        assertNull(cat.rowCount(bar))
    }

    @Test
    fun `a table only one partition has still resolves`() {
        val cat = DatabaseTableCatalog(listOf(tableCatalog(foo to listOf("_id")), tableCatalog()))

        assertEquals(setOf("_id"), cat.getTypes(foo)?.keys)
        assertEquals(setOf(foo), cat.types.keys)
    }
}
