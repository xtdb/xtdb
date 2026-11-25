package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.IntVector
import xtdb.operator.join.BuildSideMap.Companion.hashBits
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class BuildSideMapTest {

    @Test
    fun testHashBits() {
        assertEquals(0, hashBits(0))
        assertEquals(1, hashBits(1))
        assertEquals(6, hashBits(20))
        assertEquals(8, hashBits(100))
        assertEquals(15, hashBits(10000))
        assertEquals(32, hashBits(Int.MAX_VALUE))
    }

    private fun BuildSideMap.getMatches(hash: Int): List<Int> {
        val matches = mutableListOf<Int>()
        for (idx in iterator(hash)) {
            matches.add(idx)
        }
        return matches
    }

    @Test
    fun testBasicPutAndGet(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(100)
            hashCol.writeInt(200)
            hashCol.writeInt(300)
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(0), map.getMatches(100))
                assertEquals(listOf(1), map.getMatches(200))
                assertEquals(listOf(2), map.getMatches(300))
            }
        }
    }

    @Test
    fun testNonExistentHash(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(100)
            hashCol.writeInt(200)
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertTrue(map.getMatches(999).isEmpty())
            }
        }
    }

    @Test
    fun testCollisionHandling(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            val sameHash = 12345
            hashCol.writeInt(sameHash)
            hashCol.writeInt(sameHash)
            hashCol.writeInt(sameHash)
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(0, 1, 2), map.getMatches(sameHash).sorted())
            }
        }
    }

    @Test
    fun testEmptyMap(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertTrue(map.getMatches(100).isEmpty())
            }
        }
    }

    @Test
    fun testSingleElement(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(42)
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(0), map.getMatches(42))
                assertTrue(map.getMatches(99).isEmpty())
            }
        }
    }

    @Test
    fun testLargeDataset(al: BufferAllocator) {
        val size = 1000
        IntVector.open(al, false).use { hashCol ->
            repeat(size) { i -> hashCol.writeInt(i * 7) }
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(50), map.getMatches(350))
                assertEquals(listOf(100), map.getMatches(700))
                assertTrue(map.getMatches(1).isEmpty())
            }
        }
    }

    @Test
    fun testQuadraticProbing(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            repeat(20) { i -> hashCol.writeInt(i) }
            
            BuildSideMap.from(al, hashCol, 0, 0.5).use { map ->
                repeat(20) { i ->
                    assertEquals(listOf(i), map.getMatches(i), "Failed to find element $i")
                }
            }
        }
    }

    @Test
    fun testZeroHash(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(0)
            hashCol.writeInt(100)
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(0), map.getMatches(0))
            }
        }
    }

    @Test
    fun testNegativeHashes(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(-100)
            hashCol.writeInt(-200)
            hashCol.writeInt(Int.MIN_VALUE)
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(0), map.getMatches(-100))
                assertEquals(listOf(1), map.getMatches(-200))
                assertEquals(listOf(2), map.getMatches(Int.MIN_VALUE))
            }
        }
    }

    @Test
    fun testMaxIntHash(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(Int.MAX_VALUE)
            hashCol.writeInt(Int.MAX_VALUE - 1)
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(0), map.getMatches(Int.MAX_VALUE))
                assertEquals(listOf(1), map.getMatches(Int.MAX_VALUE - 1))
            }
        }
    }

    @Test
    fun testMixedCollisionsAndUniqueHashes(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(1000)  // unique
            hashCol.writeInt(2000)  // collision group
            hashCol.writeInt(2000)  // collision group
            hashCol.writeInt(3000)  // unique
            hashCol.writeInt(2000)  // collision group
            
            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(0), map.getMatches(1000))
                assertEquals(listOf(1, 2, 4), map.getMatches(2000).sorted())
                assertEquals(listOf(3), map.getMatches(3000))
                assertTrue(map.getMatches(9999).isEmpty())
            }
        }
    }

    @Test
    fun testFindValueAndRemoveOnMatch(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(1000)  // unique
            hashCol.writeInt(2000)  // collision group
            hashCol.writeInt(2000)  // collision group
            hashCol.writeInt(3000)  // unique
            hashCol.writeInt(2000)  // collision group

            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(0, map.findValue(1000, {_ -> 1}, false))
                assertEquals(2, map.findValue(2000, {idx -> if (idx == 2) 1 else -1}, true))
                assertEquals(-1, map.findValue(2000, {idx -> if (idx == 2) 1 else -1}, true))
                assertEquals(1, map.findValue(2000, {idx -> if (idx == 1) 1 else -1}, true))
                assertEquals(4, map.findValue(2000, {idx -> if (idx == 4) 1 else -1}, true))
                assertEquals(3, map.findValue(3000, {idx -> if (idx == 3) 1 else -1}, true))
            }
        }
    }

    @Test
    fun testQuadraticProbingCycles(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            hashCol.writeInt(0)
            hashCol.writeInt(0)

            BuildSideMap.from(al, hashCol, 0).use { map ->
                assertEquals(listOf(0, 1), map.getMatches(0))
            }
        }
    }

    private fun partHashes(partBits: Int, partIdx: Int, count: Int) =
        List(count) { i -> (i * 0x9E3779B1.toInt()) shl partBits or partIdx }

    @Test
    fun testShiftedPutAndGet(al: BufferAllocator) {
        val hashes = partHashes(4, 5, 1000)

        IntVector.open(al, false).use { hashCol ->
            hashes.forEach { hashCol.writeInt(it) }

            BuildSideMap.from(al, hashCol, 4).use { map ->
                hashes.forEachIndexed { idx, hash ->
                    assertEquals(listOf(idx), map.getMatches(hash), "hash $hash")
                    assertEquals(idx, map.findValue(hash, { 1 }, false), "hash $hash")
                }
            }
        }
    }

    @Test
    fun testShiftedMiss(al: BufferAllocator) {
        IntVector.open(al, false).use { hashCol ->
            partHashes(4, 5, 100).forEach { hashCol.writeInt(it) }

            BuildSideMap.from(al, hashCol, 4).use { map ->
                partHashes(4, 5, 200).drop(100).forEach { hash ->
                    assertTrue(map.getMatches(hash).isEmpty(), "hash $hash")
                    assertEquals(-1, map.findValue(hash, { 1 }, false), "hash $hash")
                }
            }
        }
    }

    @Test
    fun testShiftedCollisionsAndRemoveOnMatch(al: BufferAllocator) {
        val (unique, shared) = partHashes(4, 5, 2)

        IntVector.open(al, false).use { hashCol ->
            listOf(shared, unique, shared, shared).forEach { hashCol.writeInt(it) }

            BuildSideMap.from(al, hashCol, 4).use { map ->
                assertEquals(listOf(0, 2, 3), map.getMatches(shared).sorted())
                assertEquals(listOf(1), map.getMatches(unique))

                assertEquals(2, map.findValue(shared, { idx -> if (idx == 2) 1 else -1 }, true))
                assertEquals(-1, map.findValue(shared, { idx -> if (idx == 2) 1 else -1 }, true))
                assertEquals(0, map.findValue(shared, { idx -> if (idx == 0) 1 else -1 }, true))
                assertEquals(3, map.findValue(shared, { idx -> if (idx == 3) 1 else -1 }, true))
                assertEquals(1, map.findValue(unique, { 1 }, true))
            }
        }
    }
}
