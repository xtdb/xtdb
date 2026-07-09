package xtdb.arrow

import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

/**
 * `appendRangeTo` (the direct, copier-free append) must be behaviour-equivalent to
 * `rowCopier(dest).copyRange(...)`. Each case copies the same source both ways and compares.
 */
@ExtendWith(AllocatorResolver::class)
class AppendRangeTest {

    /** Copies `src[offset, offset+len)` via the copier and via appendRangeTo into fresh dests, asserts equal. */
    private fun assertAppendMatchesCopier(
        al: BufferAllocator, src: VectorReader, offset: Int, len: Int, newDest: () -> Vector
    ) {
        newDest().use { viaCopier ->
            newDest().use { viaAppend ->
                src.rowCopier(viaCopier).copyRange(offset, len)
                src.appendRangeTo(viaAppend, offset, len)

                viaAppend.asList shouldBe viaCopier.asList
                viaAppend.valueCount shouldBe viaCopier.valueCount
            }
        }
    }

    private fun ranges(n: Int) = listOf(0 to n, 0 to 0, 1 to (n - 1).coerceAtLeast(0), (n / 2) to (n - n / 2))

    @Test
    fun `fixed-width whole and sub-range`(al: BufferAllocator) {
        IntVector.open(al, "src", true).use { src ->
            listOf(1, 2, null, 4, 5).forEach { if (it == null) src.writeNull() else src.writeInt(it) }
            for ((off, len) in ranges(src.valueCount))
                assertAppendMatchesCopier(al, src, off, len) { IntVector.open(al, "d", true) }
        }
    }

    @Test
    fun `variable-width whole and sub-range`(al: BufferAllocator) {
        Utf8Vector(al, "src", true).use { src ->
            listOf("alpha", "b", null, "delta", "").forEach {
                if (it == null) src.writeNull() else src.writeObject(it)
            }
            for ((off, len) in ranges(src.valueCount))
                assertAppendMatchesCopier(al, src, off, len) { Utf8Vector(al, "d", true) }
        }
    }

    @Test
    fun `bit whole and sub-range`(al: BufferAllocator) {
        BitVector(al, "src", true).use { src ->
            listOf(true, false, null, true).forEach { if (it == null) src.writeNull() else src.writeBoolean(it) }
            for ((off, len) in ranges(src.valueCount))
                assertAppendMatchesCopier(al, src, off, len) { BitVector(al, "d", true) }
        }
    }

    @Test
    fun `struct whole and sub-range`(al: BufferAllocator) {
        StructVector(al, "src", true).use { src ->
            src.writeObject(mapOf("a" to 1L, "b" to "x"))
            src.writeObject(mapOf("a" to 2L, "b" to "y"))
            src.writeObject(mapOf("a" to 3L, "b" to "z"))
            for ((off, len) in ranges(src.valueCount))
                assertAppendMatchesCopier(al, src, off, len) { StructVector(al, "d", true) }
        }
    }

    @Test
    fun `struct with missing dest column back-fills nulls`(al: BufferAllocator) {
        StructVector(al, "src", true).use { src ->
            src.writeObject(mapOf("a" to 1L))
            src.writeObject(mapOf("a" to 2L))

            // dest already has a 'b' column; src has none, so appended rows should get null 'b'
            StructVector(al, "viaCopier", true).use { viaCopier ->
                StructVector(al, "viaAppend", true).use { viaAppend ->
                    listOf(viaCopier, viaAppend).forEach {
                        it.writeObject(mapOf("a" to 9L, "b" to "seed"))
                    }
                    src.rowCopier(viaCopier).copyRange(0, 2)
                    src.appendRangeTo(viaAppend, 0, 2)

                    viaAppend.asList shouldBe viaCopier.asList
                }
            }
        }
    }

    @Test
    fun `dense union whole and sub-range`(al: BufferAllocator) {
        DenseUnionVector(al, "src", listOf(LongVector(al, "i64", true), Utf8Vector(al, "utf8", true))).use { src ->
            val i64 = src.vectorFor("i64"); val utf8 = src.vectorFor("utf8")
            i64.writeLong(1); utf8.writeObject("two"); i64.writeLong(3); utf8.writeObject("four"); i64.writeLong(5)

            for ((off, len) in ranges(src.valueCount))
                assertAppendMatchesCopier(al, src, off, len) {
                    DenseUnionVector(al, "d", listOf(LongVector(al, "i64", true), Utf8Vector(al, "utf8", true)))
                }
        }
    }

    @Test
    fun `op-shape dense union with struct put leg`(al: BufferAllocator) {
        // mirrors the log-relation `op` column: a DUV whose 'put' leg is a struct of doc columns
        fun newOpVec() = DenseUnionVector(
            al, "op",
            listOf(StructVector(al, "put", false), NullVector("delete"))
        )

        newOpVec().use { src ->
            val put = src.vectorFor("put")
            put.writeObject(mapOf("_id" to 1L, "name" to "a"))
            put.writeObject(mapOf("_id" to 2L, "name" to "b"))
            src.vectorFor("delete").writeNull()
            put.writeObject(mapOf("_id" to 3L, "name" to "c"))

            for ((off, len) in ranges(src.valueCount))
                assertAppendMatchesCopier(al, src, off, len, ::newOpVec)
        }
    }

    @Test
    fun `mono source absorbed into dense union leg`(al: BufferAllocator) {
        LongVector(al, "src", true).use { src ->
            listOf(1L, null, 3L).forEach { if (it == null) src.writeNull() else src.writeLong(it) }
            for ((off, len) in ranges(src.valueCount))
                assertAppendMatchesCopier(al, src, off, len) {
                    DenseUnionVector(al, "d", listOf(LongVector(al, "i64", true)))
                }
        }
    }

    @Test
    fun `list whole and sub-range`(al: BufferAllocator) {
        ListVector(al, "src", true, IntVector.open(al, "\$data$", true)).use { src ->
            src.writeObject(listOf(1, 2, 3))
            src.writeObject(listOf<Int>())
            src.writeObject(listOf(4))
            src.writeObject(listOf(5, 6))
            for ((off, len) in ranges(src.valueCount))
                assertAppendMatchesCopier(al, src, off, len) {
                    ListVector(al, "d", true, IntVector.open(al, "\$data$", true))
                }
        }
    }

    @Test
    fun `RelationAsStructReader appended into struct`(al: BufferAllocator) {
        IntVector.open(al, "a", false).use { a ->
            Utf8Vector(al, "b", false).use { b ->
                a.writeInt(1); a.writeInt(2); a.writeInt(3)
                b.writeObject("x"); b.writeObject("y"); b.writeObject("z")
                val rel = RelationReader.from(listOf(a, b), 3)
                val src = RelationAsStructReader("docs", rel)

                StructVector(al, "viaCopier", false).use { viaCopier ->
                    StructVector(al, "viaAppend", false).use { viaAppend ->
                        src.rowCopier(viaCopier).run { copyRow(0); copyRow(1); copyRow(2) }
                        src.appendRangeTo(viaAppend, 0, 3)

                        viaAppend.asList shouldBe viaCopier.asList
                        viaAppend.valueCount shouldBe 3
                    }
                }
            }
        }
    }
}
