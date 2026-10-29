package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Encoding
import xtdb.arrow.NULL_TYPE
import xtdb.arrow.Relation
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.RunEndWidth
import xtdb.trie.Trie.dataRelSchema
import java.nio.ByteBuffer

class IidRunsCopierTest {

    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    private val runs = Encoding.RunEnd(RunEndWidth.I32)

    private fun iid(b: Int) = ByteArray(16) { b.toByte() }

    private fun openData(encoding: Encoding, vararg iids: Int, firstRow: Long = 0) =
        Relation(al, dataRelSchema(null, encoding)).also { rel ->
            iids.forEachIndexed { i, b ->
                rel["_iid"].writeBytes(ByteBuffer.wrap(iid(b)))
                rel["_system_from"].writeLong(firstRow + i)
                rel["_valid_from"].writeLong(0)
                rel["_valid_to"].writeLong(Long.MAX_VALUE)
                rel["op"].vectorFor("delete", NULL_TYPE, true).writeNull()
                rel.endRow()
            }
        }

    private val Relation.runEnds
        get() = (this["_iid"] as RunEndEncodedVector).let { iid -> (0..<iid.runCount).map { iid.runEnd(it) } }

    private val Relation.iids get() = List(rowCount) { (this["_iid"].getObject(it) as ByteArray)[0].toInt() }

    private val Relation.systemFroms get() = List(rowCount) { this["_system_from"].getLong(it) }

    @Test
    fun `equal neighbouring iids copy in as one run, alongside the rest of the row`() {
        openData(Encoding.Plain, 1, 1, 1, 2, 3, 3).use { src ->
            openData(runs).use { dest ->
                src.rowCopierFor(dest).copyRange(0, src.rowCount)

                assertEquals(listOf(3, 4, 6), dest.runEnds)
                assertEquals(src.iids, dest.iids)
                assertEquals(src.systemFroms, dest.systemFroms)
            }
        }
    }

    @Test
    fun `iids read from an encoded source copy in as runs`() {
        openData(runs, 1, 1, 2, 2, 2).use { src ->
            openData(runs).use { dest ->
                src.rowCopierFor(dest).copyRows(intArrayOf(0, 1, 2, 4))

                assertEquals(listOf(2, 4), dest.runEnds)
                assertEquals(listOf(0L, 1L, 2L, 4L), dest.systemFroms)
            }
        }
    }

    @Test
    fun `rows of one iid from two sources extend one run`() {
        openData(Encoding.Plain, 1, 2, firstRow = 0).use { src0 ->
            openData(Encoding.Plain, 2, 3, firstRow = 10).use { src1 ->
                openData(runs).use { dest ->
                    val copier0 = src0.rowCopierFor(dest)
                    val copier1 = src1.rowCopierFor(dest)

                    copier0.copyRow(0)
                    copier0.copyRow(1)
                    copier1.copyRow(0)
                    copier1.copyRow(1)

                    assertEquals(listOf(1, 2, 2, 3), dest.iids)
                    assertEquals(listOf(1, 3, 4), dest.runEnds)
                    assertEquals(listOf(0L, 1L, 10L, 11L), dest.systemFroms)
                }
            }
        }
    }

    @Test
    fun `an iid's rows alternating between two destinations extend each one's run`() {
        openData(Encoding.Plain, 1, 1, 1, 1).use { src ->
            openData(runs).use { current ->
                openData(runs).use { historical ->
                    val toCurrent = src.rowCopierFor(current)
                    val toHistorical = src.rowCopierFor(historical)

                    toCurrent.copyRow(0)
                    toHistorical.copyRow(1)
                    toCurrent.copyRow(2)
                    toHistorical.copyRow(3)

                    assertEquals(listOf(2), current.runEnds)
                    assertEquals(listOf(2), historical.runEnds)
                    assertEquals(listOf(0L, 2L), current.systemFroms)
                }
            }
        }
    }

    @Test
    fun `a selection that isn't contiguous coalesces its neighbours`() {
        openData(Encoding.Plain, 1, 2, 1, 1, 3).use { src ->
            openData(runs).use { dest ->
                src.rowCopierFor(dest).copyRows(intArrayOf(0, 2, 3, 4))

                assertEquals(listOf(1, 1, 1, 3), dest.iids)
                assertEquals(listOf(3, 4), dest.runEnds)
            }
        }
    }

    @Test
    fun `a cleared destination starts a fresh run`() {
        openData(Encoding.Plain, 1, 1).use { src ->
            openData(runs).use { dest ->
                val copier = src.rowCopierFor(dest)

                copier.copyRow(0)
                dest.clear()
                copier.copyRow(1)

                assertEquals(listOf(1), dest.runEnds)
                assertEquals(1, dest.rowCount)
            }
        }
    }
}
