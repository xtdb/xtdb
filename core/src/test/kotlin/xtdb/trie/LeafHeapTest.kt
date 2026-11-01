package xtdb.trie

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
import xtdb.util.closeAll
import java.nio.ByteBuffer

class LeafHeapTest {

    // keys from 8 up set the iid's sign bit; `tag` tells apart events that share an iid and a system-time.
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

    private fun <R> withHeap(pages: List<List<Event>>, f: (LeafHeap) -> R): R =
        RootAllocator().use { al ->
            val rels = pages.map { Trie.openLogDataWriter(al) }
            try {
                rels.zip(pages).forEach { (rel, evs) ->
                    evs.sortedWith(compareBy(servedOrder) { it.served }).forEach { rel.writeEvent(it) }
                }

                f(LeafHeap(rels.mapIndexed { idx, rel -> LeafPointer(EventRowPointer(rel, ByteArray(0)), idx) }))
            } finally {
                rels.closeAll()
            }
        }

    private fun LeafHeap.drain(): List<Event> =
        buildList {
            while (true) {
                val leafPtr = top() ?: break
                add(leafPtr.event)
                leafPtr.evPtr.nextIndex()
                reseatTop()
            }
        }

    private val pagesArb = Arb.bind(Arb.int(1..6), Arb.list(Arb.bind(Arb.int(0..15), Arb.long(0L..6L)) { k, st -> k to st }, 0..60)) { pageCount, evs ->
        val events = evs.mapIndexed { idx, (key, sysFrom) -> Event(key, sysFrom, idx) }
        List(pageCount) { p -> events.filter { it.tag % pageCount == p } }
    }

    @Test
    fun `serves every page's events by iid, then newest system-time first`() {
        val pages = listOf(
            listOf(Event(3, 5, 0), Event(12, 2, 1)),
            listOf(Event(3, 3, 2), Event(9, 4, 3)),
            listOf(Event(9, 1, 4))
        )

        assertEquals(
            listOf(Served(3, 5), Served(3, 3), Served(9, 4), Served(9, 1), Served(12, 2)),
            withHeap(pages) { it.drain() }.map { it.served })
    }

    @Test
    fun `a heap over empty pages has no top`() {
        assertNull(withHeap(listOf(emptyList(), emptyList())) { it.top() })
    }

    @Test
    fun `serves each event of every page once, in event order`() = runTest {
        checkAll(500, pagesArb) { pages ->
            val served = withHeap(pages) { it.drain() }

            assertEquals(pages.flatten().map { it.served }.sortedWith(servedOrder), served.map { it.served })
            assertEquals(pages.flatten().sortedBy { it.tag }, served.sortedBy { it.tag })
        }
    }
}
