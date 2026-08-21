package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VectorType.Companion.I64
import xtdb.trie.EventRowPointer
import xtdb.trie.Trie
import xtdb.util.TemporalBounds
import xtdb.util.TemporalDimension
import xtdb.util.closeAll
import java.nio.ByteBuffer
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

/**
 * Both resolvers are handed the same events and the same as-of/as-of bounds, which leaves the resolver
 * as the only variable — no page filtering and no planner in the way, and no need for an oracle that
 * asks a differently-bounded question and then argues the difference away.
 */
@Tag("property")
class AsOfResolverTest : SimulationTestBase() {

    private data class Event(
        val iid: Int, val sysFrom: Long,
        val validFrom: Long, val validTo: Long,
        val op: String, val x: Long
    )

    private val validTimes = listOf(0L, 10L, 20L, 30L, 40L)

    private val colNames = listOf("_iid", "x", "_valid_from", "_valid_to", "_system_from", "_system_to")

    private fun Relation.writeEvent(ev: Event) {
        this["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16) { ev.iid.toByte() }))
        this["_system_from"].writeLong(ev.sysFrom)
        this["_valid_from"].writeLong(ev.validFrom)
        this["_valid_to"].writeLong(ev.validTo)

        if (ev.op == "put")
            this["op"].vectorFor("put", STRUCT_TYPE, false).vectorFor("x", I64.arrowType, false).writeLong(ev.x)
        else this["op"][ev.op].writeNull()

        endRow()
    }

    private fun generateEvents(): List<Event> {
        val entityCount = rand.nextInt(1, 4)
        val eventCount = rand.nextInt(1, 16)

        return List(eventCount) { idx ->
            val vfIdx = rand.nextInt(validTimes.size)
            val vtIdx = rand.nextInt(vfIdx, validTimes.size)

            Event(
                iid = rand.nextInt(entityCount),

                // drawn from a range narrower than the event count, so ties are common: two events
                // sharing a system-time have to resolve the same way on either path
                sysFrom = rand.nextInt(1, eventCount + 1).toLong(),

                validFrom = validTimes[vfIdx],

                // `vtIdx == vfIdx` gives an empty range, which must not move either bracketing bound
                validTo = if (vtIdx == validTimes.size - 1 && rand.nextBoolean()) MAX_LONG else validTimes[vtIdx],

                op = when (rand.nextInt(10)) {
                    in 0..6 -> "put"
                    in 7..8 -> "delete"
                    else -> "erase"
                },

                x = idx.toLong()
            )
        }
    }

    /** Splits the events across pages, each of which has to be ordered iid ascending, system-from descending. */
    private fun pageUp(events: List<Event>, pageCount: Int): List<List<Event>> {
        val sorted = events.sortedWith(compareBy({ it.iid }, { -it.sysFrom }))

        // round-robin, so an entity's events land on several pages and the cross-page merge does some work.
        // a subsequence of a sorted list is still sorted, so each page keeps the order the pointers need.
        return List(pageCount) { p -> sorted.filterIndexed { idx, _ -> idx % pageCount == p } }
    }

    private fun resolveWith(
        resolver: EntityResolver, al: BufferAllocator, readers: List<RelationReader>
    ): List<Pair<Int, Map<*, *>>> {
        val pointers = readers.mapIndexedNotNull { idx, rel ->
            EventRowPointer(rel, ByteArray(0)).takeIf { it.isValid() }?.let { LeafPointer(it, idx) }
        }

        val merge = EntityMerge(pointers)

        return BitemporalConsumer.open(al, readers, colNames).use { out ->
            while (merge.nextEntity()) resolver.resolveEntity(merge, out)

            // the page a row came from is part of the answer: both resolvers should pick not just the same
            // winning event but the same copy of it
            out.build().flatMapIndexed { relIdx, rel -> rel.asMaps.map { relIdx to it } }
        }
    }

    @RepeatableSimulationTest
    fun `as-of resolution agrees with the polygon resolver`() {
        val events = generateEvents()
        val pages = pageUp(events, rand.nextInt(1, 4))

        // offset off the boundaries as well as onto them, so the point falls inside, between and outside ranges
        val validTime = validTimes.random(rand) + rand.nextInt(-5, 6)
        val systemTime = rand.nextInt(1, events.size + 2).toLong()

        val bounds = TemporalBounds(TemporalDimension.at(validTime), TemporalDimension.at(systemTime))
        val clampValidTime = rand.nextBoolean()

        RootAllocator().use { al ->
            val rels = pages.map { Trie.openLogDataWriter(al) }

            try {
                rels.zip(pages).forEach { (rel, pageEvents) -> pageEvents.forEach { rel.writeEvent(it) } }

                // half the time, reach the rows through an indirection, as an iid-selected page does.
                // ascending, because that's what the selectors emit and what keeps equal iids contiguous
                val select = rand.nextBoolean()
                val readers = rels.map { rel ->
                    if (!select) rel
                    else rel.select((0 until rel.rowCount).filter { rand.nextInt(4) > 0 }.toIntArray())
                }

                assertEquals(
                    resolveWith(PolygonResolver(bounds, clampValidTime), al, readers),
                    resolveWith(AsOfResolver(bounds, clampValidTime), al, readers),
                    "seed=$currentSeed, V=$validTime, S=$systemTime, clamp=$clampValidTime, select=$select, pages=$pages"
                )
            } finally {
                rels.closeAll()
            }
        }
    }
}
