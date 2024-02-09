package xtdb.bitemporal

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import com.carrotsearch.hppc.LongArrayList.from as longs
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

/*
 When I say 'rectangle' in here, I'm talking about a graph where sys-time is the y-axis, valid-time is the x-axis
 */

internal class IPolygonReaderTest {

    @Test
    fun `test single-rectangle recency`() {
        assertEquals(
            MAX_LONG,
            Polygon(longs(3, MAX_LONG), longs(MAX_LONG)).recency,
            "current"
        )

        assertEquals(
            10,
            Polygon(longs(4, 10), longs(MAX_LONG)).recency,
            "put for valid-time range"
        )

        assertEquals(
            4, Polygon(longs(6, 10), longs(4)).recency,
            "vt=tt passes above the rectangle"
        )

        assertEquals(
            6, Polygon(longs(6, 10), longs(6)).recency,
            "vt=tt touches the top-left corner"
        )

        assertEquals(
            8, Polygon(longs(6, 10), longs(8)).recency,
            "vt=tt hits the top of the rectangle"
        )

        assertEquals(
            10, Polygon(longs(6, 10), longs(10)).recency,
            "vt=tt touches the top-right corner"
        )

        assertEquals(
            10, Polygon(longs(6, 10), longs(12)).recency,
            "vt=tt hits the RHS of the rectangle"
        )
    }

    @Test
    fun `test polygon recency`() {
        assertEquals(
            5,
            Polygon(longs(3, 5, MAX_LONG), longs(MAX_LONG, 5)).recency,
            "standard vt=tt replacement"
        )

        assertEquals(
            6,
            Polygon(longs(3, 5, MAX_LONG), longs(MAX_LONG, 6)).recency,
            "retroactively corrected"
        )

        assertEquals(
            7,
            Polygon(longs(3, 7, MAX_LONG), longs(MAX_LONG, 6)).recency,
            "scheduled correction"
        )

        assertEquals(
            4,
            Polygon(longs(1, 4), longs(5)).recency,
            "historical/historical"
        )

        assertEquals(
            8,
            Polygon(longs(10, 12, 15, 18), longs(8, 6, 3)).recency,
            "sys-time descending but all with vt>tt"
        )

        assertEquals(
            8,
            Polygon(longs(10, 12, 15, 18), longs(6, 8, 3)).recency,
            "sys-time oscillating but all with vt>tt"
        )

        assertEquals(
            4,
            Polygon(longs(0, 2, 5, 8), longs(7, 4, 2)).recency,
            "sys-times descending, intersect halfway through the middle rectangle"
        )

        assertEquals(
            6,
            Polygon(longs(100, 100, 5, 8), longs(100, 9, 6)).recency,
            "sys-times descending, intersect halfway through the final rectangle - demonstrates a short circuit"
        )
    }
}
