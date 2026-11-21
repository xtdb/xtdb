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
    fun `a catalog round-trips through its tables' block files, each holding its own table's indices, in every state, tries included`() {
        val dropAt = Instant.parse("2026-01-02T03:04:05.123456Z")
        val byEmail = def("by_email")
        val bySku = def("by_sku", table = "orders", column = "sku")
        val byName = def("by_name", column = "name")
        val byEmailL0s = listOf(IndexTrie(Trie.l0Key(3), 1024), IndexTrie(Trie.l0Key(4), 2048))
        val bySkuL0 = IndexTrie(Trie.l0Key(5), 512)

        val catalog = IndexCatalog.EMPTY
            .withIndex(byEmail, 3, setOf(2L, 0L, 1L))
            .withIndex(bySku, 5, emptySet())
            .withIndex(byName, 5, emptySet())
            .ready(byName.id)
            .withL0s(mapOf(byEmail.id to byEmailL0s[0]))
            .withL0s(mapOf(byEmail.id to byEmailL0s[1], bySku.id to bySkuL0))
            .dropped(bySku.id, dropAt)

        assertEquals(
            setOf(
                SecondaryIndex(byEmail, 3, IndexState.Building(setOf(0L, 1L, 2L)), byEmailL0s),
                SecondaryIndex(bySku, 5, IndexState.Dropped(dropAt), listOf(bySkuL0)),
                SecondaryIndex(byName, 5, IndexState.Ready, emptyList()),
            ),
            IndexCatalog.fromProto(catalog.tables.associateWith { catalog.toProto(it) }).indices.toSet(),
        )
        assertEquals(
            listOf(bySku.id.toString()),
            catalog.toProto(bySku.table).indicesList.map { it.definition.indexId },
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
        val catalog = IndexCatalog.EMPTY.withIndex(def("by_email"), 0, emptySet())

        val fault = assertThrows<Fault> { catalog.withIndex(def("by_email"), 0, emptySet()) }
        assertEquals(Keyword.intern("xtdb.index", "index-name-declared"), fault.errorCode())
    }

    @Test
    fun `declaring an id already in the catalog faults, even once dropped`() {
        val byEmail = def("by_email")
        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet()).dropped(byEmail.id, Instant.EPOCH)

        val fault = assertThrows<Fault> { catalog.withIndex(byEmail, 1, emptySet()) }
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
        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet()).dropped(byEmail.id, Instant.EPOCH)

        val fault = assertThrows<Fault> { catalog.dropped(byEmail.id, Instant.EPOCH) }
        assertEquals(Keyword.intern("xtdb.index", "index-already-dropped"), fault.errorCode())
    }

    @Test
    fun `a complete Building index becomes Ready, and applying Ready again changes nothing`() {
        val byEmail = def("by_email")
        val ready = IndexCatalog.EMPTY.withIndex(byEmail, 3, emptySet()).ready(byEmail.id)

        assertEquals(IndexState.Ready, ready[byEmail.id]?.state)
        assertEquals(ready.indices.toList(), ready.ready(byEmail.id).indices.toList())
    }

    @Test
    fun `Ready on an index dropped since changes nothing`() {
        val byEmail = def("by_email")
        val dropped = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet()).dropped(byEmail.id, Instant.EPOCH)

        assertEquals(dropped.indices.toList(), dropped.ready(byEmail.id).indices.toList())
    }

    @Test
    fun `Ready on an index still owing blocks faults`() {
        val byEmail = def("by_email")
        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 2, setOf(0L, 1L))

        val fault = assertThrows<Fault> { catalog.ready(byEmail.id) }
        assertEquals(Keyword.intern("xtdb.index", "index-not-complete"), fault.errorCode())
    }

    @Test
    fun `Ready on an index the catalog does not hold faults`() {
        val fault = assertThrows<Fault> { IndexCatalog.EMPTY.ready(UUID.randomUUID()) }
        assertEquals(Keyword.intern("xtdb.index", "unknown-index"), fault.errorCode())
    }

    @Test
    fun `a backfill strikes its block from those owed, and adds the L0 it wrote`() {
        val byEmail = def("by_email")
        val l0 = IndexTrie(Trie.l0Key(1), 1024)

        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 3, setOf(0L, 1L, 2L))
            .backfilled(byEmail.id, 1, l0)
            .backfilled(byEmail.id, 0, null)

        assertEquals(SecondaryIndex(byEmail, 3, IndexState.Building(setOf(2L)), listOf(l0)), catalog[byEmail.id])
    }

    @Test
    fun `a backfill of a block no longer owed changes nothing`() {
        val byEmail = def("by_email")
        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 2, setOf(0L, 1L)).backfilled(byEmail.id, 0, IndexTrie(Trie.l0Key(0), 1))

        assertEquals(
            catalog.indices.toList(),
            catalog.backfilled(byEmail.id, 0, IndexTrie(Trie.l0Key(0), 1)).backfilled(byEmail.id, 2, null).indices.toList(),
            "a repeated block, and one at or above the first flushed block",
        )
    }

    @Test
    fun `a backfill of a Ready or dropped index changes nothing`() {
        val byEmail = def("by_email")
        val bySku = def("by_sku", table = "orders", column = "sku")
        val catalog = IndexCatalog.EMPTY
            .withIndex(byEmail, 2, emptySet()).ready(byEmail.id)
            .withIndex(bySku, 2, setOf(0L, 1L)).dropped(bySku.id, Instant.EPOCH)

        assertEquals(
            catalog.indices.toList(),
            catalog.backfilled(byEmail.id, 0, IndexTrie(Trie.l0Key(0), 1)).backfilled(bySku.id, 0, IndexTrie(Trie.l0Key(0), 1)).indices.toList(),
        )
    }

    @Test
    fun `a backfill for an index the catalog does not hold faults`() {
        val fault = assertThrows<Fault> { IndexCatalog.EMPTY.backfilled(UUID.randomUUID(), 0, null) }
        assertEquals(Keyword.intern("xtdb.index", "unknown-index"), fault.errorCode())
    }
}
