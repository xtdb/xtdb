package xtdb.bitemporal

import com.carrotsearch.hppc.LongArrayList.from as longs
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VectorType.Companion.I64
import xtdb.trie.EventRowPointer
import xtdb.trie.Trie
import xtdb.util.TemporalBounds
import xtdb.util.TemporalDimension
import java.nio.ByteBuffer
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

/**
 * Events are written in the order bitemporal resolution sees them: iid ascending, system-from descending.
 */
class PolygonCalculatorTest {

    private val iidA = ByteArray(16) { 1 }
    private val iidB = ByteArray(16) { 2 }

    private fun Relation.writeEvent(iid: ByteArray, sysFrom: Long, validFrom: Long, validTo: Long, op: String) {
        this["_iid"].writeBytes(ByteBuffer.wrap(iid))
        this["_system_from"].writeLong(sysFrom)
        this["_valid_from"].writeLong(validFrom)
        this["_valid_to"].writeLong(validTo)

        if (op == "put") this["op"].vectorFor("put", STRUCT_TYPE, false).vectorFor("x", I64.arrowType, false).writeLong(0)
        else this["op"][op].writeNull()

        endRow()
    }

    private fun withEvents(
        queryBounds: TemporalBounds? = null,
        writeEvents: Relation.() -> Unit,
        assertPolygons: (PolygonCalculator, EventRowPointer) -> Unit
    ) =
        RootAllocator().use { al ->
            Trie.openLogDataWriter(al).use { rel ->
                rel.writeEvents()
                assertPolygons(PolygonCalculator(queryBounds), EventRowPointer(rel, ByteArray(0)))
            }
        }

    @Test
    fun `an earlier event resolves against only its own entity's ceiling`() {
        withEvents(
            writeEvents = {
                writeEvent(iidA, sysFrom = 2, validFrom = 10, validTo = 20, op = "put")
                writeEvent(iidA, sysFrom = 1, validFrom = 0, validTo = 100, op = "put")
                writeEvent(iidB, sysFrom = 1, validFrom = 0, validTo = 100, op = "put")
            },
            assertPolygons = { calc, evPtr ->
                calc.calculate(evPtr)
                evPtr.nextIndex()

                calc.calculate(evPtr).let { polygon ->
                    assertEquals(longs(0, 10, 20, 100), polygon!!.validTimes)
                    assertEquals(longs(MAX_LONG, 2, MAX_LONG), polygon.sysTimeCeilings)
                }
                evPtr.nextIndex()

                calc.calculate(evPtr).let { polygon ->
                    assertEquals(longs(0, 100), polygon!!.validTimes, "B's ceiling is untouched by A's events")
                    assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
                }
            }
        )
    }

    @Test
    fun `an erase suppresses the entity's earlier events, and only that entity's`() {
        withEvents(
            writeEvents = {
                writeEvent(iidA, sysFrom = 2, validFrom = 0, validTo = MAX_LONG, op = "erase")
                writeEvent(iidA, sysFrom = 1, validFrom = 0, validTo = 100, op = "put")
                writeEvent(iidB, sysFrom = 1, validFrom = 0, validTo = 100, op = "put")
            },
            assertPolygons = { calc, evPtr ->
                calc.calculate(evPtr)
                evPtr.nextIndex()

                assertNull(calc.calculate(evPtr), "A's put is erased")
                evPtr.nextIndex()

                assertEquals(longs(0, 100), calc.calculate(evPtr)!!.validTimes, "B is untouched by A's erase")
            }
        )
    }

    @Test
    fun `an erase after the query's system time still suppresses`() {
        withEvents(
            queryBounds = TemporalBounds(systemTime = TemporalDimension(1, 2)),
            writeEvents = {
                writeEvent(iidA, sysFrom = 5, validFrom = 0, validTo = MAX_LONG, op = "erase")
                writeEvent(iidA, sysFrom = 1, validFrom = 0, validTo = 100, op = "put")
            },
            assertPolygons = { calc, evPtr ->
                calc.calculate(evPtr)
                evPtr.nextIndex()

                assertNull(calc.calculate(evPtr))
            }
        )
    }

    @Test
    fun `a put after the query's system time neither resolves nor constrains`() {
        withEvents(
            queryBounds = TemporalBounds(systemTime = TemporalDimension(1, 3)),
            writeEvents = {
                writeEvent(iidA, sysFrom = 5, validFrom = 10, validTo = 20, op = "put")
                writeEvent(iidA, sysFrom = 1, validFrom = 0, validTo = 100, op = "put")
            },
            assertPolygons = { calc, evPtr ->
                assertNull(calc.calculate(evPtr))
                evPtr.nextIndex()

                calc.calculate(evPtr).let { polygon ->
                    assertEquals(longs(0, 100), polygon!!.validTimes, "the out-of-bounds put left no ceiling behind")
                    assertEquals(longs(MAX_LONG), polygon.sysTimeCeilings)
                }
            }
        )
    }
}
