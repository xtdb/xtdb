package xtdb.index

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.TableRef
import xtdb.api.error.Fault
import java.time.Instant
import java.util.UUID

class IndexCatalogTest {
    private val errorCodeKey = Keyword.intern("xtdb.error", "code")

    private fun def(name: String, table: String = "users", column: String = "email") =
        IndexDef(UUID.randomUUID(), name, TableRef("public", table), column)

    private fun Fault.errorCode() = data.valAt(errorCodeKey)

    @Test
    fun `a catalog round-trips through its tables' block files, each holding its own table's indices, dropped ones included`() {
        val dropAt = Instant.parse("2026-01-02T03:04:05.123456Z")
        val byEmail = def("by_email")
        val bySku = def("by_sku", table = "orders", column = "sku")

        val catalog = IndexCatalog.EMPTY
            .withIndex(byEmail, 3)
            .withIndex(bySku, 5)
            .dropped(bySku.id, dropAt)

        assertEquals(
            setOf(
                SecondaryIndex(byEmail, 3, IndexState.Building),
                SecondaryIndex(bySku, 5, IndexState.Dropped(dropAt)),
            ),
            IndexCatalog.fromProto(catalog.tables.associateWith { catalog.toProto(it) }).indices.toSet(),
        )
        assertEquals(
            listOf(byEmail.id.toString()),
            catalog.toProto(byEmail.table).indicesList.map { it.definition.indexId },
        )
    }

    @Test
    fun `declaring a name already declared faults`() {
        val catalog = IndexCatalog.EMPTY.withIndex(def("by_email"), 0)

        val fault = assertThrows<Fault> { catalog.withIndex(def("by_email"), 0) }
        assertEquals(Keyword.intern("xtdb.index", "index-name-declared"), fault.errorCode())
    }

    @Test
    fun `declaring an id already in the catalog faults, even once dropped`() {
        val byEmail = def("by_email")
        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 0).dropped(byEmail.id, Instant.EPOCH)

        val fault = assertThrows<Fault> { catalog.withIndex(byEmail, 1) }
        assertEquals(Keyword.intern("xtdb.index", "index-id-exists"), fault.errorCode())
    }

    @Test
    fun `dropping an index the catalog does not hold faults`() {
        val fault = assertThrows<Fault> { IndexCatalog.EMPTY.dropped(UUID.randomUUID(), Instant.EPOCH) }
        assertEquals(Keyword.intern("xtdb.index", "unknown-index"), fault.errorCode())
    }

    @Test
    fun `dropping an index twice faults`() {
        val byEmail = def("by_email")
        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 0).dropped(byEmail.id, Instant.EPOCH)

        val fault = assertThrows<Fault> { catalog.dropped(byEmail.id, Instant.EPOCH) }
        assertEquals(Keyword.intern("xtdb.index", "index-already-dropped"), fault.errorCode())
    }
}
