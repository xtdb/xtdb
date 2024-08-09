package xtdb.util

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class TemporalBoundsTest {
   @Test
    fun `test temporalBounds`() {
        // b1 - [0, 10], [10, 20], [0, 10], [10, 20]
        val b1 = TemporalBounds(
            TemporalColumn(0, 10),
            TemporalColumn(10 , 20),
            TemporalColumn(0, 10),
            TemporalColumn(10, 20))

       // b2 - [5, 15], [15, 25], [5, 15], [15, 25] coming after b1 but intersects
       val b2 = TemporalBounds(
            TemporalColumn(5, 15),
            TemporalColumn(15 , 25),
            TemporalColumn(5, 15),
            TemporalColumn(15, 25))

       // b3 - [11, 20], [15, 25], [5, 15], [15, 25] coming after only intersection in sys-time, overlaps in valid-time
       val b3 = TemporalBounds(
           TemporalColumn(11, 20),
           TemporalColumn(15 , 25),
           TemporalColumn(5, 15),
           TemporalColumn(15, 25))

      // b4 - [-10, -5], [-5, -1], [15, 25], [25, 35] coming before b1 can not bound valid time
       val b4 = TemporalBounds(
           TemporalColumn(-10, -5),
           TemporalColumn( -5 , -1),
           TemporalColumn(15, 25),
           TemporalColumn(25, 35))

       assertTrue(b1.intersects(b2))
       assertTrue(b2.intersects(b1))

       assertFalse(b1.intersects(b3))
       assertFalse(b3.intersects(b1))

       assertFalse(b1.intersectsValidTime(b3))
       assertFalse(b3.intersectsValidTime(b1))

       assertTrue(b1.overlaps(b3))
       assertTrue(b3.overlaps(b1))

       assertTrue(b1.overlapsValidTime(b3))
       assertTrue(b3.overlapsValidTime(b1))

       assertTrue(b1.canBoundValidTime(b2))
       assertFalse(b4.canBoundValidTime(b1))
    }
}