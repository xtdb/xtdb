package xtdb.operator.scan

import io.kotest.property.Arb
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation
import xtdb.trie.EventRowPointer
import xtdb.trie.Trie
import xtdb.util.closeAll
import java.nio.ByteBuffer

class EntityMergeTest {

    // `key`'s top two bits are its iid's level-0 trie bucket, and keys from 8 up set the iid's sign bit.
    // `tag` tells apart events that share an iid and a system-time.
    private data class Event(val key: Int, val sysFrom: Long, val tag: Int)

    private data class Served(val key: Int, val sysFrom: Long)

    private val Event.served get() = Served(key, sysFrom)

    private val servedOrder = compareBy<Served>({ it.key }, { -it.sysFrom })

    private fun Relation.writeEvent(ev: Event) {
        this["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16).also { it[0] = (ev.key shl 4).toByte() }))
        this["_system_from"].writeLong(ev.sysFrom)
        this["_valid_from"].writeLong(ev.key * 1000L + ev.tag)
        this["_valid_to"].writeLong(Long.MAX_VALUE)
        this["op"]["delete"].writeNull()
        endRow()
    }

    private val LeafPointer.event
        get() = evPtr.validFrom.let { vf -> Event((vf / 1000).toInt(), evPtr.systemFrom, (vf % 1000).toInt()) }

    private fun <R> withMerge(pages: List<List<Event>>, path: ByteArray = ByteArray(0), f: (EntityMerge) -> R): R =
        RootAllocator().use { al ->
            val rels = pages.map { Trie.openLogDataWriter(al) }
            try {
                rels.zip(pages).forEach { (rel, evs) ->
                    evs.sortedWith(compareBy(servedOrder) { it.served }).forEach { rel.writeEvent(it) }
                }

                val pointers = rels.mapIndexedNotNull { idx, rel ->
                    EventRowPointer(rel, path).takeIf { it.isValid() }?.let { LeafPointer(it, idx) }
                }

                f(EntityMerge(pointers))
            } finally {
                rels.closeAll()
            }
        }

    private fun EntityMerge.serveAll(eventsPerEntity: Int = Int.MAX_VALUE): List<List<Event>> =
        buildList {
            while (nextEntity()) {
                add(buildList {
                    while (size < eventsPerEntity) add((nextEvent() ?: break).event)
                })
            }
        }

    private val pagesArb = Arb.bind(Arb.int(1..6), Arb.list(Arb.bind(Arb.int(0..15), Arb.long(0L..6L)) { k, st -> k to st }, 0..60)) { pageCount, evs ->
        val events = evs.mapIndexed { idx, (key, sysFrom) -> Event(key, sysFrom, idx) }
        List(pageCount) { p -> events.filter { it.tag % pageCount == p } }
    }

    private fun expected(pages: List<List<Event>>, keep: (Event) -> Boolean = { true }) =
        pages.flatten().filter(keep).groupBy { it.key }.toSortedMap().values.map { evs -> evs.sortedByDescending { it.sysFrom } }

    @Test
    fun `serves each entity once, its events newest system-time first, across pages`() {
        val pages = listOf(
            listOf(Event(3, 2, 0), Event(9, 5, 1)),
            listOf(Event(3, 4, 2), Event(12, 1, 3)),
            listOf(Event(3, 3, 4), Event(9, 1, 5))
        )

        assertEquals(
            listOf(
                listOf(Served(3, 4), Served(3, 3), Served(3, 2)),
                listOf(Served(9, 5), Served(9, 1)),
                listOf(Served(12, 1))
            ),
            withMerge(pages) { it.serveAll() }.map { evs -> evs.map { it.served } })
    }

    @Test
    fun `serves no event before the first entity`() {
        assertNull(withMerge(listOf(listOf(Event(0, 1, 0)))) { it.nextEvent() })
    }

    @Test
    fun `serves every event in iid then reverse system-time order`() = runTest {
        checkAll(500, pagesArb) { pages ->
            val served = withMerge(pages) { it.serveAll() }

            assertEquals(expected(pages).map { evs -> evs.map { it.served } }, served.map { evs -> evs.map { it.served } })
            assertEquals(pages.flatten().sortedBy { it.tag }, served.flatten().sortedBy { it.tag })
        }
    }

    @Test
    fun `a resolver that stops early still sees each entity once, from its newest event`() = runTest {
        checkAll(500, pagesArb, Arb.int(0..3)) { pages, eventsPerEntity ->
            val served = withMerge(pages) { it.serveAll(eventsPerEntity) }

            assertEquals(
                expected(pages).map { evs -> evs.take(eventsPerEntity).map { it.served } },
                served.map { evs -> evs.map { it.served } })
        }
    }

    @Test
    fun `serves only the rows on the task's path`() = runTest {
        checkAll(500, pagesArb, Arb.int(0..3)) { pages, bucket ->
            val served = withMerge(pages, byteArrayOf(bucket.toByte())) { it.serveAll() }

            assertEquals(
                expected(pages) { it.key shr 2 == bucket }.map { evs -> evs.map { it.served } },
                served.map { evs -> evs.map { it.served } })
        }
    }
}
