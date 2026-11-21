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

    private fun IndexCatalog.roundTripped() = IndexCatalog.fromProto(tables.associateWith { toProto(it) })

    @Test
    fun `a catalog round-trips through its tables' block files, each holding its own table's indices, in every state, tries included`() {
        val dropAt = Instant.parse("2026-01-02T03:04:05.123456Z")
        val byEmail = def("by_email")
        val bySku = def("by_sku", table = "orders", column = "sku")
        val byName = def("by_name", column = "name")
        val byTotal = def("by_total", table = "orders", column = "total")
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
            .withIndex(byTotal, 5, emptySet())
            .dropped(byTotal.id, dropAt)
            .withoutGarbage(byTotal.id, emptySet())

        val roundTripped = catalog.roundTripped()
        assertEquals(
            setOf(
                SecondaryIndex(byEmail, 3, IndexState.Building(setOf(0L, 1L, 2L)), byEmailL0s.associateWith { IndexTrieState.Live }),
                SecondaryIndex(bySku, 5, IndexState.Dropped(dropAt), mapOf(bySkuL0 to IndexTrieState.Garbage(dropAt))),
                SecondaryIndex(byName, 5, IndexState.Ready, emptyMap()),
            ),
            roundTripped.indices.toSet(),
        )
        assertEquals(
            listOf(bySku.id.toString()),
            catalog.toProto(bySku.table).indicesList.map { it.definition.indexId },
        )
        assertEquals(
            listOf(byTotal.id.toString()),
            catalog.toProto(byTotal.table).collectedIdsList,
            "a collected id stays in the file of the table its index was on",
        )
        assertEquals(setOf(byTotal.id), roundTripped.collectedIds)
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
    fun `dropping an index twice faults, whether or not it holds files, and once it is collected`() {
        val byEmail = def("by_email")
        val holding = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet())
            .withL0s(mapOf(byEmail.id to IndexTrie(Trie.l0Key(0), 1))).dropped(byEmail.id, Instant.EPOCH)
        val empty = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet()).dropped(byEmail.id, Instant.EPOCH)
        val collected = empty.withoutGarbage(byEmail.id, emptySet())

        for (catalog in listOf(holding, empty, collected)) {
            val fault = assertThrows<Fault> { catalog.dropped(byEmail.id, Instant.EPOCH) }
            assertEquals(Keyword.intern("xtdb.index", "index-already-dropped"), fault.errorCode())
        }
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

        assertEquals(SecondaryIndex(byEmail, 3, IndexState.Building(setOf(2L)), mapOf(l0 to IndexTrieState.Live)), catalog[byEmail.id])
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

    private fun l1(block: Long) = Trie.Key(1, null, null, block)

    @Test
    fun `a compaction makes its outputs Live and its inputs Garbage, in one step`() {
        val byEmail = def("by_email")
        val asOf = Instant.parse("2026-01-02T03:04:05Z")
        val l0s = listOf(IndexTrie(Trie.l0Key(0), 10), IndexTrie(Trie.l0Key(1), 20))
        val out = IndexTrie(l1(1), 25)

        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet())
            .withL0s(mapOf(byEmail.id to l0s[0])).withL0s(mapOf(byEmail.id to l0s[1]))
            .compacted(byEmail.id, listOf(out), l0s.map { it.key }, asOf)

        assertEquals(
            mapOf(l0s[0] to IndexTrieState.Garbage(asOf), l0s[1] to IndexTrieState.Garbage(asOf), out to IndexTrieState.Live),
            catalog[byEmail.id]?.tries,
        )
        assertEquals(listOf(out), catalog[byEmail.id]?.liveTries)
        assertEquals(catalog.indices.toSet(), catalog.roundTripped().indices.toSet(), "Garbage tries persist with their dates")
    }

    @Test
    fun `a compaction whose inputs are not all still Live changes nothing`() {
        val byEmail = def("by_email")
        val l0s = (0L..2L).map { IndexTrie(Trie.l0Key(it), 10) }
        val catalog = l0s.fold(IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet())) { cat, l0 -> cat.withL0s(mapOf(byEmail.id to l0)) }
            .compacted(byEmail.id, listOf(IndexTrie(l1(1), 15)), listOf(l0s[0].key, l0s[1].key), Instant.EPOCH)

        assertEquals(
            catalog.indices.toList(),
            catalog
                .compacted(byEmail.id, listOf(IndexTrie(l1(1), 15)), listOf(l0s[0].key, l0s[1].key), Instant.EPOCH.plusSeconds(1))
                .compacted(byEmail.id, listOf(IndexTrie(l1(2), 20)), listOf(l0s[1].key, l0s[2].key), Instant.EPOCH.plusSeconds(1))
                .compacted(byEmail.id, listOf(IndexTrie(l1(9), 20)), listOf(Trie.l0Key(9)), Instant.EPOCH.plusSeconds(1))
                .indices.toList(),
            "a duplicate publish, one with an input another job replaced, and one with an input never added",
        )
    }

    @Test
    fun `adding a trie whose key the index already holds changes nothing, whatever its state`() {
        val byEmail = def("by_email")
        val l0 = IndexTrie(Trie.l0Key(0), 10)
        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet()).withL0s(mapOf(byEmail.id to l0))

        assertEquals(catalog.indices.toList(), catalog.withL0s(mapOf(byEmail.id to IndexTrie(Trie.l0Key(0), 99))).indices.toList())

        val compacted = catalog.compacted(byEmail.id, listOf(IndexTrie(l1(0), 10)), listOf(l0.key), Instant.EPOCH)
        assertEquals(compacted.indices.toList(), compacted.withL0s(mapOf(byEmail.id to l0)).indices.toList(), "a Garbage trie is not made Live again")
    }

    @Test
    fun `a compaction for an index the catalog does not hold faults`() {
        val fault = assertThrows<Fault> { IndexCatalog.EMPTY.compacted(UUID.randomUUID(), emptyList(), emptyList(), Instant.EPOCH) }
        assertEquals(Keyword.intern("xtdb.index", "unknown-index"), fault.errorCode())
    }

    @Test
    fun `a drop makes every trie Garbage as of the drop, keeping an earlier compaction's date`() {
        val byEmail = def("by_email")
        val compactedAt = Instant.parse("2026-01-01T00:00:00Z")
        val dropAt = Instant.parse("2026-01-02T00:00:00Z")
        val l0 = IndexTrie(Trie.l0Key(0), 10)
        val out = IndexTrie(l1(0), 10)

        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet()).withL0s(mapOf(byEmail.id to l0))
            .compacted(byEmail.id, listOf(out), listOf(l0.key), compactedAt)
            .dropped(byEmail.id, dropAt)

        assertEquals(
            SecondaryIndex(byEmail, 0, IndexState.Dropped(dropAt), mapOf(l0 to IndexTrieState.Garbage(compactedAt), out to IndexTrieState.Garbage(dropAt))),
            catalog[byEmail.id],
        )
    }

    @Test
    fun `an index dropped holding no tries stays Dropped until GC's empty deletion collects it, and its id can't be declared again`() {
        val byEmail = def("by_email")
        val dropped = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet()).dropped(byEmail.id, Instant.EPOCH)

        assertEquals(listOf(SecondaryIndex(byEmail, 0, IndexState.Dropped(Instant.EPOCH), emptyMap())), dropped.indices.toList())
        assertEquals(emptySet<UUID>(), dropped.collectedIds)

        val collected = dropped.withoutGarbage(byEmail.id, emptySet())
        assertEquals(emptyList<SecondaryIndex>(), collected.indices.toList())
        assertEquals(setOf(byEmail.id), collected.collectedIds)

        val fault = assertThrows<Fault> { collected.withIndex(byEmail, 1, emptySet()) }
        assertEquals(Keyword.intern("xtdb.index", "index-id-exists"), fault.errorCode())
    }

    @Test
    fun `GC's garbage is each index's Garbage tries dated at or before its cutoff`() {
        val byEmail = def("by_email")
        val bySku = def("by_sku", table = "orders", column = "sku")
        val compactedAt = Instant.parse("2026-01-01T00:00:00Z")
        val l0s = (0L..1L).map { IndexTrie(Trie.l0Key(it), 10) }

        val catalog = IndexCatalog.EMPTY
            .withIndex(byEmail, 0, emptySet()).withL0s(mapOf(byEmail.id to l0s[0])).withL0s(mapOf(byEmail.id to l0s[1]))
            .compacted(byEmail.id, listOf(IndexTrie(l1(1), 20)), l0s.map { it.key }, compactedAt)
            .withIndex(bySku, 0, emptySet()).withL0s(mapOf(bySku.id to l0s[0]))

        assertEquals(emptyMap<SecondaryIndex, List<Trie.Key>>(), catalog.garbage(compactedAt.minusNanos(1000)))
        assertEquals(mapOf(catalog[byEmail.id] to l0s.map { it.key }), catalog.garbage(compactedAt), "index L0s included, and no Live trie")
    }

    @Test
    fun `GC's garbage holds a dropped index with no tries, with no keys, once its drop is at or before the cutoff`() {
        val byEmail = def("by_email")
        val dropAt = Instant.parse("2026-01-01T00:00:00Z")
        val catalog = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet()).dropped(byEmail.id, dropAt)

        assertEquals(emptyMap<SecondaryIndex, List<Trie.Key>>(), catalog.garbage(dropAt.minusNanos(1000)))
        assertEquals(mapOf(catalog[byEmail.id] to emptyList<Trie.Key>()), catalog.garbage(dropAt))
    }

    @Test
    fun `GC's deletion removes only Garbage tries, and a dropped index's last takes it out of the catalog`() {
        val byEmail = def("by_email")
        val l0s = (0L..1L).map { IndexTrie(Trie.l0Key(it), 10) }
        val out = IndexTrie(l1(1), 20)

        val compacted = IndexCatalog.EMPTY.withIndex(byEmail, 0, emptySet())
            .withL0s(mapOf(byEmail.id to l0s[0])).withL0s(mapOf(byEmail.id to l0s[1]))
            .compacted(byEmail.id, listOf(out), l0s.map { it.key }, Instant.EPOCH)

        val collected = compacted.withoutGarbage(byEmail.id, setOf(l0s[0].key, out.key))
        assertEquals(
            mapOf(l0s[1] to IndexTrieState.Garbage(Instant.EPOCH), out to IndexTrieState.Live),
            collected[byEmail.id]?.tries,
            "the Live output stays",
        )

        val dropped = collected.dropped(byEmail.id, Instant.EPOCH.plusSeconds(1))
        assertEquals(setOf(byEmail.id), dropped.indices.map { it.def.id }.toSet(), "a dropped index stays while it holds files")

        val gone = dropped.withoutGarbage(byEmail.id, setOf(l0s[1].key)).withoutGarbage(byEmail.id, setOf(out.key))
        assertEquals(emptyList<SecondaryIndex>(), gone.indices.toList())
        assertEquals(setOf(byEmail.id), gone.collectedIds)
        assertEquals(gone.collectedIds, gone.roundTripped().collectedIds, "collected ids persist")
    }

    @Test
    fun `a late message for a collected index changes nothing, where one for an id never declared faults`() {
        val byEmail = def("by_email")
        val collected = IndexCatalog.EMPTY.withIndex(byEmail, 1, setOf(0L)).dropped(byEmail.id, Instant.EPOCH)
            .withoutGarbage(byEmail.id, emptySet())

        val late = collected
            .ready(byEmail.id)
            .backfilled(byEmail.id, 0, IndexTrie(Trie.l0Key(0), 1))
            .compacted(byEmail.id, listOf(IndexTrie(l1(0), 1)), listOf(Trie.l0Key(0)), Instant.EPOCH)
            .withoutGarbage(byEmail.id, setOf(Trie.l0Key(0)))

        assertEquals(emptyList<SecondaryIndex>(), late.indices.toList())
        assertEquals(setOf(byEmail.id), late.collectedIds)

        val fault = assertThrows<Fault> { IndexCatalog.EMPTY.withoutGarbage(UUID.randomUUID(), emptySet()) }
        assertEquals(Keyword.intern("xtdb.index", "unknown-index"), fault.errorCode())
    }
}
