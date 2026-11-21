package xtdb.index

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.TableRef
import xtdb.api.error.Fault
import xtdb.trie.Trie
import java.time.Instant
import java.util.UUID

class IndexCatalogTest {
    private val errorCodeKey = Keyword.intern("xtdb.error", "code")

    private fun def(name: String, table: String = "users", column: String = "email") =
        IndexDef(UUID.randomUUID(), name, TableRef("public", table), column)

    private fun Fault.errorCode() = data.valAt(errorCodeKey)

    @Test
    fun `a catalog round-trips through its tables' block files, each holding its own table's indices, tries and dropped ones included`() {
        val dropAt = Instant.parse("2026-01-02T03:04:05.123456Z")
        val byEmail = def("by_email")
        val bySku = def("by_sku", table = "orders", column = "sku")
        val byEmailL0s = listOf(IndexTrie(Trie.l0Key(3), 1024), IndexTrie(Trie.l0Key(4), 2048))
        val bySkuL0 = IndexTrie(Trie.l0Key(5), 512)

        val catalog = IndexCatalog.EMPTY
            .withIndex(byEmail, 3)
            .withIndex(bySku, 5)
            .withL0s(mapOf(byEmail.id to byEmailL0s[0]))
            .withL0s(mapOf(byEmail.id to byEmailL0s[1], bySku.id to bySkuL0))
            .dropped(bySku.id, dropAt)

        assertEquals(
            setOf(
                SecondaryIndex(byEmail, 3, IndexState.Building, byEmailL0s),
                SecondaryIndex(bySku, 5, IndexState.Dropped(dropAt), listOf(bySkuL0)),
            ),
            IndexCatalog.fromProto(catalog.tables.associateWith { catalog.toProto(it) }).indices.toSet(),
        )
        assertEquals(
            listOf(byEmail.id.toString()),
            catalog.toProto(byEmail.table).indicesList.map { it.definition.indexId },
        )
    }

    @Test
    fun `adding an L0 for an index the catalog does not hold faults`() {
        val fault = assertThrows<Fault> {
            IndexCatalog.EMPTY.withL0s(mapOf(UUID.randomUUID() to IndexTrie(Trie.l0Key(0), 1)))
        }
        assertEquals(Keyword.intern("xtdb.index", "unknown-index"), fault.errorCode())
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
