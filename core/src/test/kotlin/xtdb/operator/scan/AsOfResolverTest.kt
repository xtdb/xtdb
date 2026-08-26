package xtdb.operator.scan

import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.choose
import io.kotest.property.arbitrary.constant
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.map
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.STRUCT_TYPE
import xtdb.trie.EventRowPointer
import xtdb.trie.Trie
import xtdb.util.TemporalBounds
import xtdb.util.TemporalDimension
import xtdb.util.closeAll
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZonedDateTime
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

/**
 * Both resolvers are handed the same events and the same as-of/as-of bounds, which leaves the resolver
 * as the only variable — no page filtering and no planner in the way, and no need for an oracle that
 * asks a differently-bounded question and then argues the difference away.
 */
class AsOfResolverTest {

    companion object {
        private const val ITERATIONS = 500

        private val VALID_TIMES = listOf(0L, 10L, 20L, 30L, 40L)

        private val COL_NAMES = listOf("_iid", "x", "_valid_from", "_valid_to", "_system_from", "_system_to")
    }

    private data class Event(
        val iid: Int, val sysFrom: Long,
        val validFrom: Long, val validTo: Long,
        val op: String, val x: Long
    )

    /** `dropEvery` of 0 reads the pages directly; otherwise rows are reached through an indirection. */
    private data class Case(
        val events: List<Event>, val pageCount: Int,
        val validTime: Long, val systemTime: Long,
        val clampValidTime: Boolean, val dropEvery: Int
    )

    /** One emitted row, with the page it came from — both resolvers should agree on the copy, not just the event. */
    private data class Row(val pageIdx: Int, val cols: Map<*, *>)

    private fun Relation.writeEvent(ev: Event) {
        this["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16) { ev.iid.toByte() }))
        this["_system_from"].writeLong(ev.sysFrom)
        this["_valid_from"].writeLong(ev.validFrom)
        this["_valid_to"].writeLong(ev.validTo)

        // `writeObject` rather than writing the struct's child directly: writing the grandchild leaves the
        // struct row unfinished, so the union's leg is never set and `op` doesn't read back as "put"
        if (ev.op == "put") this["op"].vectorFor("put", STRUCT_TYPE, false).writeObject(mapOf("x" to ev.x))
        else this["op"][ev.op].writeNull()

        endRow()
    }

    /** Splits the events across pages, each of which has to be ordered iid ascending, system-from descending. */
    private fun pageUp(events: List<Event>, pageCount: Int): List<List<Event>> {
        val sorted = events.sortedWith(compareBy({ it.iid }, { -it.sysFrom }))

        // one entity's events at one system-time come from a single transaction, so they reach a scan
        // through a single trie and cannot be split across pages. Splitting them would make the merge's
        // tie-break — arbitrary between equal system-times — depend on how the pointers happened to be
        // queued, which differs between a resolver that drains an entity and one that exits early.
        val txGroups = sorted.groupBy { it.iid to it.sysFrom }.values.toList()

        // round-robin the groups, so an entity's events still land on several pages and the cross-page
        // merge does some work. Groups are in sorted order, so each page stays sorted.
        return List(pageCount) { p -> txGroups.filterIndexed { idx, _ -> idx % pageCount == p }.flatten() }
    }

    private fun resolveWith(resolver: EntityResolver, al: BufferAllocator, readers: List<RelationReader>): List<Row> {
        val pointers = readers.mapIndexedNotNull { idx, rel ->
            EventRowPointer(rel, ByteArray(0)).takeIf { it.isValid() }?.let { LeafPointer(it, idx) }
        }

        val merge = EntityMerge(pointers)

        return BitemporalConsumer.open(al, readers, COL_NAMES).use { out ->
            while (merge.nextEntity()) resolver.resolveEntity(merge, out)

            out.build().flatMapIndexed { pageIdx, rel ->
                // `_iid` arrives as a ByteArray, which compares by reference, so it's listified
                rel.asMaps.map { row -> Row(pageIdx, row.mapValues { (_, v) -> if (v is ByteArray) v.toList() else v }) }
            }
        }
    }

    private fun resolveBoth(case: Case): Pair<List<Row>, List<Row>> {
        val pages = pageUp(case.events, case.pageCount)
        val bounds = TemporalBounds(TemporalDimension.at(case.validTime), TemporalDimension.at(case.systemTime))

        return RootAllocator().use { al ->
            val rels = pages.map { Trie.openLogDataWriter(al) }

            try {
                rels.zip(pages).forEach { (rel, pageEvents) -> pageEvents.forEach { rel.writeEvent(it) } }

                // ascending, because that's what the iid selectors emit and what keeps equal iids contiguous
                val readers: List<RelationReader> =
                    if (case.dropEvery == 0) rels
                    else rels.map { rel -> rel.select((0 until rel.rowCount).filter { it % case.dropEvery != 0 }.toIntArray()) }

                resolveWith(PolygonResolver(bounds, case.clampValidTime), al, readers) to
                        resolveWith(AsOfResolver(bounds, case.clampValidTime), al, readers)
            } finally {
                rels.closeAll()
            }
        }
    }

    private val eventArb: Arb<Event> = arbitrary {
        val vfIdx = Arb.int(VALID_TIMES.indices).bind()
        val vtIdx = Arb.int(vfIdx..VALID_TIMES.lastIndex).bind()
        val openEnded = vtIdx == VALID_TIMES.lastIndex && Arb.boolean().bind()

        Event(
            iid = Arb.int(0..2).bind(),

            // drawn from a range narrower than the event count, so ties are common: two events sharing a
            // system-time have to resolve the same way on either path
            sysFrom = Arb.long(1L..8L).bind(),

            validFrom = VALID_TIMES[vfIdx],

            // `vtIdx == vfIdx` gives an empty range, which must not move either bracketing bound
            validTo = if (openEnded) MAX_LONG else VALID_TIMES[vtIdx],

            op = Arb.choose(
                7 to Arb.constant("put"), 2 to Arb.constant("delete"), 1 to Arb.constant("erase")
            ).bind(),

            x = 0
        )
    }

    // `Arb.bind`, not an `arbitrary { }` block: the block's draws don't shrink, and a counterexample of
    // sixteen events is unreadable until the list has been shrunk. `eventArb` above has to stay a block,
    // its valid-to being drawn from a range that depends on its valid-from.
    private val caseArb: Arb<Case> = Arb.bind(
        Arb.list(eventArb, 1..16).map { evs -> evs.mapIndexed { idx, ev -> ev.copy(x = idx.toLong()) } },
        Arb.int(1..3),

        // offset off the boundaries as well as onto them, so the point falls inside, between and outside
        Arb.bind(Arb.element(VALID_TIMES), Arb.int(-5..5)) { vt, offset -> vt + offset },

        Arb.long(1L..9L),
        Arb.boolean(),
        Arb.element(0, 0, 3, 4),
        ::Case
    )

    // --- the anchors. A differential test proves nothing while both sides produce nothing, so these
    // assert concrete results; the property below only ever asserts that the two agree.

    /** system-time 1 puts `[40, 80)`; system-time 2 puts `[70, ∞)`. */
    private val clippingEvents = listOf(
        Event(iid = 0, sysFrom = 2, validFrom = 70, validTo = MAX_LONG, op = "put", x = 2),
        Event(iid = 0, sysFrom = 1, validFrom = 40, validTo = 80, op = "put", x = 1)
    )

    private fun anchorRows(events: List<Event>, validTime: Long, systemTime: Long): List<Row> {
        val case = Case(events, pageCount = 1, validTime, systemTime, clampValidTime = false, dropEvery = 0)
        val (viaPolygon, viaAsOf) = resolveBoth(case)

        assertEquals(viaPolygon, viaAsOf, "the two resolvers agree: $case")

        return viaAsOf
    }

    private fun List<Row>.soleValidTo(): Instant {
        assertEquals(1, size, "one visible row: $this")
        val cols = single().cols
        return (cols.entries.single { it.key.toString().endsWith("valid-to") }.value as ZonedDateTime).toInstant()
    }

    /**
     * A newer event the query point can't see still clips the visible row's `_valid_to` — `[70, ∞)` is not
     * itself visible as of valid-time 50, but it ends `[40, 80)` at 70. This is what `rightBound` is for.
     */
    @Test
    fun `a newer range to the right of the query point clips the winner's valid-to`() {
        assertEquals(Instant.EPOCH.plusNanos(70_000), anchorRows(clippingEvents, validTime = 50, systemTime = 3).soleValidTo())
    }

    /**
     * The same two events with the system-time bound moved below the clipping one: it now contributes
     * nothing at all, so `_valid_to` stays at the winner's own 80 rather than being pulled back to 70.
     */
    @Test
    fun `an event above the system-time bound does not clip the winner`() {
        assertEquals(Instant.EPOCH.plusNanos(80_000), anchorRows(clippingEvents, validTime = 50, systemTime = 1).soleValidTo())
    }

    @Tag("property")
    @Test
    fun `as-of resolution agrees with the polygon resolver`() = runTest {
        checkAll(ITERATIONS, caseArb) { case ->
            val (viaPolygon, viaAsOf) = resolveBoth(case)

            assertEquals(viaPolygon, viaAsOf, "$case")
        }
    }
}
