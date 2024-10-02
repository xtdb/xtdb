package xtdb.util

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class SkipListTest {

    @Test
    fun testAddAndGet() {
        val skipList = SkipList<Int>()
        skipList.add(1)
        skipList.add(2)
        skipList.add(3)
        assertEquals(1, skipList[0])
        assertEquals(2, skipList[1])
        assertEquals(3, skipList[2])
    }

    @Test
    fun testSet() {
        val skipList = SkipList<Int>()
        skipList.add(10)
        skipList.add(20)
        skipList.add(30)

        skipList[1] = 25  // Update value at index 1
        assertEquals(25, skipList[1])
        assertEquals(3, skipList.size)

        skipList[0] = 5   // Update value at index 0
        assertEquals(5, skipList[0])

        skipList[2] = 35  // Update value at index 2
        assertEquals(35, skipList[2])
    }

    @Test
    fun testSetOutOfBounds() {
        val skipList = SkipList<Int>()
        skipList.add(1)
        skipList.add(2)

        assertThrows<IndexOutOfBoundsException>() {
            skipList[-1] = 0
        }

        assertThrows<IndexOutOfBoundsException> {
            skipList[2] = 3
        }
    }

    @Test
    fun testInsert() {
        val skipList = SkipList<Int>()
        skipList.add(1)
        skipList.add(3)
        skipList.insert(1, 2) // Insert 2 at position 1
        assertEquals(1, skipList[0])
        assertEquals(2, skipList[1])
        assertEquals(3, skipList[2])
    }

    @Test
    fun testRemoveRange() {
        val skipList = SkipList<Int>()
        for (i in 1..5) {
            skipList.add(i)
        }
        skipList.removeRange(1, 4) // Removes elements at positions 1 to 3
        assertEquals(2, skipList.size)
        assertEquals(1, skipList[0])
        assertEquals(5, skipList[1])
    }

    @Test
    fun testClear() {
        val skipList = SkipList<Int>()
        for (i in 1..5) {
            skipList.add(i)
        }
        skipList.clear()
        assertEquals(0, skipList.size)
        assertThrows<IndexOutOfBoundsException>() {
            skipList[0]
        }
    }

    @Test
    fun testOutOfBoundsAccess() {
        val skipList = SkipList<Int>()
        skipList.add(1)
        assertThrows<IndexOutOfBoundsException>() {
            skipList[-1]
        }
        assertThrows<IndexOutOfBoundsException>() {
            skipList[1]
        }
    }

    @Test
    fun testInsertAtBounds() {
        val skipList = SkipList<Int>()
        skipList.add(1)
        skipList.add(2)
        skipList.insert(0, 0) // Insert at the beginning
        skipList.insert(3, 3) // Insert at the end
        assertEquals(0, skipList[0])
        assertEquals(1, skipList[1])
        assertEquals(2, skipList[2])
        assertEquals(3, skipList[3])
    }

    @Test
    fun testRemoveRangeFull() {
        val skipList = SkipList<Int>()
        for (i in 1..5) {
            skipList.add(i)
        }
        skipList.removeRange(0, 5) // Removes all elements
        assertEquals(0, skipList.size)
        assertThrows<IndexOutOfBoundsException> {
            skipList[0]
        }
    }

    @Test
    fun testSequentialOperations() {
        val skipList = SkipList<String>()
        skipList.add("a")
        skipList.add("c")
        skipList.insert(1, "b") // List is now ["a", "b", "c"]
        assertEquals("b", skipList[1])
        skipList[1] = "beta" // Update value at index 1
        assertEquals("beta", skipList[1])
        skipList.removeRange(0, 2) // Removes "a" and "beta"
        assertEquals(1, skipList.size)
        assertEquals("c", skipList[0])
        skipList.clear()
        assertEquals(0, skipList.size)
        skipList.add("d")
        assertEquals("d", skipList[0])
    }

    @Test
    fun testLargeNumberOfElements() {
        val skipList = SkipList<Int>()
        val n = 1000
        for (i in 0 until n) {
            skipList.add(i)
        }
        assertEquals(n, skipList.size)
        for (i in 0 until n) {
            assertEquals(i, skipList[i])
        }
        // Update some values
        for (i in 0 until n step 100) {
            skipList[i] = -i
        }
        for (i in 0 until n) {
            if (i % 100 == 0) {
                assertEquals(-i, skipList[i])
            } else {
                assertEquals(i, skipList[i])
            }
        }
        // Remove the middle 500 elements
        skipList.removeRange(250, 750)
        assertEquals(500, skipList.size)
        for (i in 0 until 250) {
            val expected = if (i % 100 == 0) -i else i
            assertEquals(expected, skipList[i])
        }
        for (i in 250 until 500) {
            val idx = i + 500
            val expected = if (idx % 100 == 0) -idx else idx
            assertEquals(expected, skipList[i])
        }
    }

    @Test
    fun testRandomOperations() {
        val skipList = SkipList<Int>()
        val list = mutableListOf<Int>()
        val rand = kotlin.random.Random(0)
        for (i in 0 until 100) {
            val value = rand.nextInt(1000)
            val pos = rand.nextInt(0, skipList.size + 1)
            skipList.insert(pos, value)
            list.add(pos, value)
        }
        // Randomly update some values
        for (i in 0 until 50) {
            val index = rand.nextInt(0, skipList.size)
            val newValue = rand.nextInt(1000, 2000)
            skipList[index] = newValue
            list[index] = newValue
        }
        for (i in 0 until skipList.size) {
            assertEquals(list[i], skipList[i])
        }
    }

    @Test
    fun testComparsion() {
        val skipList1 = SkipList<Int>()
        val skipList2 = SkipList<Int>()
        skipList1.add(1)
        skipList1.add(2)
        skipList1.add(3)
        skipList2.add(1)
        skipList2.add(2)
        skipList2.add(3)
        assertEquals(skipList1, skipList2)
        skipList2.add(4)
        assertEquals(-1, skipList1.compareTo(skipList2))
        assertEquals(1, skipList2.compareTo(skipList1))
    }
}