package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.hash.MurmurHasher
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.test.AllocatorResolver
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.schema
import xtdb.util.Hasher
import kotlin.io.path.exists

@ExtendWith(AllocatorResolver::class)
class ShuffleTest {

    private fun hashInt(i: Int) = MurmurHasher.combineHashCode(0, Hasher.Xx().hash(i.toDouble()))

    private fun Shuffle.writeIds(rel: Relation, ids: List<Int>) {
        rel.clear()
        for (id in ids) {
            rel.writeRow(mapOf("id" to id))
        }
        shuffle()
    }

    private fun Shuffle.toIds(rel: Relation): List<List<Int>> = buildList {
        for (i in 0 until partCount) {
            loadDataPart(rel, i)
            add(rel.toMaps(SNAKE_CASE_STRING).map { it["id"] as Int })
        }
    }

    private fun Shuffle.toHashes(hashCol: Vector): List<List<Int>> = buildList {
        for (i in 0 until partCount) {
            loadHashPart(hashCol, i)
            add(hashCol.asList.map { it as Int })
        }
    }

    private fun hashList(ids: List<Int>) = ids.map { hashInt(it) }

    @Test
    fun testShuffle(al: BufferAllocator) {
        Relation(al, schema("id" ofType I32)).use { rel ->
            "hashes".ofType(I32).openVector(al).use { hashCol ->
                val shuffleFiles = Shuffle.open(al, rel, listOf("id"), 8, 3).use { shuffle ->
                    shuffle.writeIds(rel, (1..3).toList())
                    shuffle.writeIds(rel, (4..6).toList())
                    shuffle.writeIds(rel, (7..8).toList())
                    shuffle.end()

                    assertEquals(4, shuffle.partCount)

                    assertEquals(
                        listOf(
                            listOf(3),
                            listOf(1, 2, 4),
                            listOf(6, 7),
                            listOf(5, 8)
                        ),
                        shuffle.toIds(rel)
                    )

                    assertEquals(
                        listOf(
                            hashList(listOf(3)),
                            hashList(listOf(1, 2, 4)),
                            hashList(listOf(6, 7)),
                            hashList(listOf(5, 8))
                        ),
                        shuffle.toHashes(hashCol)
                    )

                    listOf(shuffle.dataFile, shuffle.hashFile).apply {
                        assertTrue(all { it.exists() })
                    }
                }

                assertFalse(shuffleFiles.any { it.exists() })
            }
        }
    }

    @Test
    fun testLargerShuffle(al: BufferAllocator) {
        Relation(al, schema("id" ofType I32)).use { rel ->
            "hashes".ofType(I32).openVector(al).use { hashCol ->
                Shuffle.open(al, rel, listOf("id"), 10000, 4).use { shuffle ->
                    shuffle.writeIds(rel, (0..<2500).toList())
                    shuffle.writeIds(rel, (2500..<5000).toList())
                    shuffle.writeIds(rel, (5000..<7500).toList())
                    shuffle.writeIds(rel, (7500..<10000).toList())

                    shuffle.end()

                    assertEquals(8, shuffle.partCount)

                    assertEquals((0..<10000).toSet(), shuffle.toIds(rel).flatten().toSet())

                    assertEquals(
                        hashList((0..<10000).toList()).toSet(),
                        shuffle.toHashes(hashCol).flatten().toSet()
                    )
                }
            }
        }
    }

    @Test
    fun testEmptyShuffle(al: BufferAllocator) {
        Relation(al, schema("id" ofType I32)).use { rel ->
            "hashes".ofType(I32).openVector(al).use { hashCol ->
                Shuffle.open(al, rel, listOf("id"), 1000, 4).use { shuffle ->
                    repeat(4) { shuffle.shuffle() }
                    shuffle.end()

                    assertEquals(8, shuffle.partCount)

                    assertEquals(List(8) { emptyList<Any>() }, shuffle.toIds(rel))

                    assertEquals(List(8) { emptyList<Any>() }, shuffle.toHashes(hashCol))
                }
            }
        }
    }

    @Test
    fun testMinParts(al: BufferAllocator) {
        Relation(al, schema("id" ofType I32)).use { rel ->
            "hashes".ofType(I32).openVector(al).use { hashCol ->
                Shuffle.open(al, rel, listOf("id"), 6, 1).use { shuffle ->
                    shuffle.writeIds(rel, (0..<6).toList())
                    shuffle.end()

                    assertEquals(2, shuffle.partCount)

                    assertEquals(
                        listOf(
                            listOf(0, 3),
                            listOf(1, 2, 4, 5)
                        ),
                        shuffle.toIds(rel)
                    )

                    assertEquals(
                        listOf(
                            hashList(listOf(0, 3)),
                            hashList(listOf(1, 2, 4, 5))
                        ),
                        shuffle.toHashes(hashCol)
                    )
                }

                Shuffle.open(al, rel, listOf("id"), 6, 1, minParts = 4).use { shuffle ->
                    shuffle.writeIds(rel, (0..<6).toList())
                    shuffle.end()

                    assertEquals(4, shuffle.partCount)

                    assertEquals(
                        listOf(
                            listOf(0, 3),
                            listOf(1, 2, 4),
                            listOf(),
                            listOf(5)
                        ),
                        shuffle.toIds(rel)
                    )

                    assertEquals(
                        listOf(
                            hashList(listOf(0, 3)),
                            hashList(listOf(1, 2, 4)),
                            hashList(listOf()),
                            hashList(listOf(5))
                        ),
                        shuffle.toHashes(hashCol)
                    )
                }
            }
        }
    }
}
