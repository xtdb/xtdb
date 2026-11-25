package xtdb.arrow

import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher

class DenseUnionVectorTest {
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun testDenseUnionVector() {
        DenseUnionVector(
            allocator,
            listOf("i32", "utf8"),
            listOf(
                IntVector.open(allocator, false),
                Utf8Vector(allocator, true)
            )
        ).use { duv ->
            val i32Leg = duv.vectorFor("i32")
            val utf8Leg = duv.vectorFor("utf8")

            i32Leg.writeInt(12)
            utf8Leg.writeObject("hello")
            utf8Leg.writeObject("world!")
            i32Leg.writeInt(34)
            utf8Leg.writeNull()

            assertEquals(5, duv.valueCount)
            assertEquals(listOf(12, 34), i32Leg.asList)
            assertEquals(listOf("hello", "world!", null), utf8Leg.asList)
            assertEquals(listOf(12, "hello", "world!", 34, null), duv.asList)

            assertEquals(listOf(0, 1, 1, 0, 1).map { it.toByte() }, duv.typeIds())
            assertEquals(listOf(0, 0, 1, 1, 2), duv.offsets())
        }
    }

    @Test
    fun `a bytes leg's metadata hashes the union rows a selection picks from that leg, in selection order`() {
        DenseUnionVector(
            allocator,
            listOf("i32", "utf8"),
            listOf(
                IntVector.open(allocator, false),
                Utf8Vector(allocator, true)
            )
        ).use { duv ->
            val i32Leg = duv.vectorFor("i32")
            val utf8Leg = duv.vectorFor("utf8")

            i32Leg.writeInt(12)
            utf8Leg.writeObject("hello")
            utf8Leg.writeObject("world!")
            i32Leg.writeInt(34)
            utf8Leg.writeNull()

            val bytes = duv.metadataFlavours.filterIsInstance<MetadataFlavour.Bytes>().single()

            fun hashes(hasher: Hasher, sel: IntArray?) =
                IntVector.open(allocator, false).use { out ->
                    bytes.hashTo(out, hasher, sel)
                    out.asList
                }

            for (hasher in listOf(Hasher.Xx(), Hasher.Murmur3())) {
                assertEquals(listOf(4, 2).map { duv.hashCode(it, hasher) }, hashes(hasher, intArrayOf(4, 0, 2, 3)))
                assertEquals(listOf(1, 2, 4).map { duv.hashCode(it, hasher) }, hashes(hasher, null))
            }
        }
    }

    @Test
    fun `value reader follows each row's leg`() {
        DenseUnionVector(
            allocator,
            listOf("i32", "utf8"),
            listOf(
                IntVector.open(allocator, false),
                Utf8Vector(allocator, true)
            )
        ).use { duv ->
            val i32Leg = duv.vectorFor("i32")
            val utf8Leg = duv.vectorFor("utf8")

            i32Leg.writeInt(12)
            utf8Leg.writeObject("hello")
            utf8Leg.writeNull()
            i32Leg.writeInt(34)

            val rdr = duv.valueReader()

            fun readAt(pos: Int): Triple<String?, Boolean, Any?> {
                rdr.pos = pos
                return Triple(rdr.leg, rdr.isNull, if (rdr.isNull) null else rdr.readObject())
            }

            assertEquals(
                listOf(Triple("i32", false, 12), Triple("utf8", false, "hello"),
                    Triple("utf8", true, null), Triple("i32", false, 34)),
                listOf(0, 1, 2, 3).map(::readAt)
            )

            assertEquals(Triple("i32", false, 34), readAt(3))
            assertEquals(Triple("utf8", false, "hello"), readAt(1))
            assertEquals(34, rdr.also { it.pos = 3 }.readInt())
        }
    }

    private fun ValueReader.present() = Pair(leg, if (isNull) null else readObject())

    @Test
    fun `value reader positions across absent rows and reads the present rows after them`() {
        openDuv(listOf(12L, Undefined, "hello", Undefined, Undefined, null, 34L)).use { duv ->
            val expected = listOf(
                Pair("i64", 12L), Pair("utf8", "hello"), Pair("null", null), Pair("i64", 34L)
            )
            val presentRows = intArrayOf(0, 2, 5, 6)

            val rdr = duv.valueReader()
            assertEquals(
                expected,
                (0..<duv.valueCount).mapNotNull { idx ->
                    rdr.pos = idx
                    if (idx in presentRows) rdr.present() else null
                },
                "direct"
            )

            assertEquals(
                expected,
                duv.valueReader().asSequence().mapNotNull { row ->
                    if (row.pos in presentRows) row.present() else null
                }.toList(),
                "iterated"
            )

            val sel = intArrayOf(1, 6, 3, 0, 4, 2)
            assertEquals(
                listOf(Pair("i64", 34L), Pair("i64", 12L), Pair("utf8", "hello")),
                duv.select(sel).valueReader().asSequence().mapNotNull { row ->
                    if (sel[row.pos] in presentRows) row.present() else null
                }.toList(),
                "select(IntArray)"
            )
        }
    }

    @Test
    fun `a leg's value reader positions across other legs' rows and absent rows`() {
        DenseUnionVector(allocator).use { duv ->
            val structLeg = duv.vectorFor("struct", STRUCT_TYPE, true)
            structLeg.writeObject(mapOf("a" to 1L))
            repeat(2000) { duv.writeObject(it.toLong()) }
            duv.writeUndefined()
            structLeg.writeObject(mapOf("a" to 2L))

            val structRows = setOf(0, duv.valueCount - 1)

            val rdr = structLeg.valueReader()
            assertEquals(
                listOf(1L, 2L),
                (0..<duv.valueCount).mapNotNull { idx ->
                    rdr.pos = idx
                    assertEquals(structLeg.isNull(idx), rdr.isNull, "isNull at $idx")
                    if (idx in structRows) rdr.readStruct().getValue("a").readLong() else null
                }
            )
        }
    }

    @Test
    fun `value reader on an absent row reads as the vector's own getters do`() {
        openDuv(listOf(12L, Undefined)).use { duv ->
            val rdr = duv.valueReader().also { it.pos = 1 }

            assertNull(duv.getLeg(1))
            assertNull(rdr.leg)
            assertEquals(true, duv.isNull(1))
            assertEquals(true, rdr.isNull)

            assertThrows<IllegalStateException> { duv.getObject(1) }
            assertThrows<IllegalStateException> { rdr.readObject() }
            assertThrows<NullPointerException> { duv.getLong(1) }
            assertThrows<NullPointerException> { rdr.readLong() }
        }
    }

    @Test
    fun `scalar into DUV #3609`() {
        IntVector.open(allocator, false).use { myIntVec ->
            myIntVec.writeInt(32)
            myIntVec.writeInt(64)

            DenseUnionVector(
                allocator,
                listOf("utf8", "i32"),
                listOf(
                    Utf8Vector(allocator, false),
                    IntVector.open(allocator, true)
                )
            ).use { destVec ->
                myIntVec.rowCopier(destVec).run {
                    copyRow(1)
                    copyRow(0)
                }

                assertEquals(listOf(64, 32), destVec.asList)
            }
        }
    }

    @Test
    fun `nullable mono into DUV`() {
        DoubleVector(allocator, true).use { dblVec ->
            dblVec.writeDouble(3.14)
            dblVec.writeNull()
            dblVec.writeDouble(2.71)

            DenseUnionVector(
                allocator,
                listOf("f64"),
                listOf(DoubleVector(allocator, true))
            ).use { destVec ->
                dblVec.rowCopier(destVec).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(3.14, null, 2.71), destVec.asList)
            }
        }
    }


    @Test
    fun `from null into duv vector`() {
        NullVector().use { nullVector ->
            nullVector.writeNull()
            nullVector.writeNull()
            nullVector.writeNull()

            DenseUnionVector(
                allocator,
                listOf("i64"),
                listOf(LongVector(allocator, true))
            ).use { copy ->
                val copier = nullVector.rowCopier(copy)
                copier.copyRow(0)
                copier.copyRow(1)
                copier.copyRow(2)

                assertEquals(3, copy.valueCount)
                assertNull(copy.getObject(1))
            }
        }
    }

    @Test
    fun `from nullable duv vector to mono vector`() {
        DenseUnionVector(
            allocator,
            listOf("null", "i32"),
            listOf(
                NullVector(),
                IntVector.open(allocator, false),
            )
        ).use { duv ->
            val intLeg = duv.vectorFor("i32")
            val nullLeg = duv.vectorFor("null")
            intLeg.writeInt(12)
            nullLeg.writeNull()
            intLeg.writeInt(34)

            assertEquals(3, duv.valueCount)

            IntVector.open(allocator, true).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(12, null, 34), mono.asList)
            }
        }

        DenseUnionVector(
            allocator,
            listOf("null", "struct"),
            listOf(
                NullVector(),
                StructVector(allocator, false,
                    linkedMapOf(
                        "a" to IntVector.open(allocator, false),
                        "b" to IntVector.open(allocator, false)
                    )
                ),
            )
        ).use { duv ->
            val obj1 = mapOf("a" to 1, "b" to 2)
            val obj2 = mapOf("a" to 3, "b" to 4)

            val structLeg = duv.vectorFor("struct")
            val nullLeg = duv.vectorFor("null")
            structLeg.writeObject(obj1)
            nullLeg.writeNull()
            structLeg.writeObject(obj2)

            assertEquals(3, duv.valueCount)

            StructVector(allocator, true,
                linkedMapOf(
                    "a" to IntVector.open(allocator, false),
                    "b" to IntVector.open(allocator, false)
                )
            ).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(obj1, null, obj2), mono.asList)
            }
        }

        DenseUnionVector(
            allocator,
            listOf("null", "list"),
            listOf(
                NullVector(),
                ListVector(allocator, false, IntVector.open(allocator, false))
            ))
        .use { duv ->
            val obj1 = listOf(1, 2)
            val obj2 = emptyList<Int>()

            val structLeg = duv.vectorFor("list")
            val nullLeg = duv.vectorFor("null")
            structLeg.writeObject(obj1)
            nullLeg.writeNull()
            structLeg.writeObject(obj2)

            assertEquals(3, duv.valueCount)

            ListVector(allocator, false, IntVector.open(allocator, false)).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(obj1, null, obj2), mono.asList)
            }
        }
    }

    @Test
    fun `copy whole DUV into empty DUV`() {
        DenseUnionVector(
            allocator,
            listOf("i32", "utf8"),
            listOf(
                IntVector.open(allocator, false),
                Utf8Vector(allocator, true)
            )
        ).use { src ->
            src.vectorFor("i32").writeInt(12)
            src.vectorFor("utf8").writeObject("hello")
            src.vectorFor("i32").writeInt(34)
            src.vectorFor("utf8").writeNull()

            DenseUnionVector(
                allocator,
                listOf("i32", "utf8"),
                listOf(
                    IntVector.open(allocator, false),
                    Utf8Vector(allocator, true)
                )
            ).use { dest ->
                src.rowCopier(dest).copyRange(0, src.valueCount)

                assertEquals(src.valueCount, dest.valueCount)
                assertEquals(src.asList, dest.asList)
                assertEquals(src.typeIds(), dest.typeIds())
                assertEquals(src.offsets(), dest.offsets())
            }
        }
    }

    @Test
    fun `copy whole DUV into DUV with existing rows`() {
        DenseUnionVector(
            allocator,
            listOf("i32", "utf8"),
            listOf(
                IntVector.open(allocator, false),
                Utf8Vector(allocator, true)
            )
        ).use { src ->
            src.vectorFor("i32").writeInt(12)
            src.vectorFor("utf8").writeObject("hello")
            src.vectorFor("i32").writeInt(34)

            DenseUnionVector(
                allocator,
                listOf("i32", "utf8"),
                listOf(
                    IntVector.open(allocator, false),
                    Utf8Vector(allocator, true)
                )
            ).use { dest ->
                dest.vectorFor("utf8").writeObject("existing")
                dest.vectorFor("i32").writeInt(99)

                assertEquals(2, dest.valueCount)

                src.rowCopier(dest).copyRange(0, src.valueCount)

                assertEquals(5, dest.valueCount)
                assertEquals(listOf("existing", 99, 12, "hello", 34), dest.asList)
                assertEquals(listOf(1, 0, 0, 1, 0).map { it.toByte() }, dest.typeIds())
                assertEquals(listOf(0, 0, 1, 1, 2), dest.offsets())
            }
        }
    }

    private object Undefined

    private val arbScalar: Arb<Any?> =
        Arb.choice(
            Arb.constant(null), Arb.long(-3L..3L), Arb.string(0..3), Arb.boolean(), Arb.numericDouble(),
            Arb.list(Arb.long(-3L..3L), 0..3)
        )

    private fun arbDoc(depth: Int): Arb<Map<String, Any?>> =
        Arb.map(Arb.element("a", "b", "c"), if (depth == 0) arbScalar else Arb.choice(arbScalar, arbDoc(depth - 1)), 0, 3)

    private val arbRow: Arb<Any?> = Arb.choice(arbScalar, arbDoc(2), Arb.constant(Undefined))

    private fun openDuv(rows: List<Any?>) =
        DenseUnionVector(allocator).apply {
            for (row in rows) if (row === Undefined) writeUndefined() else writeObject(row)
        }

    private val DenseUnionVector.rows
        get() = (0..<valueCount).map { if (getTypeId(it) < 0) Undefined else getObject(it) }

    private fun checkSameValuesAsCopyRow(srcRows: List<Any?>, destRows: List<Any?>, sel: IntArray, copy: (RowCopier) -> Unit) {
        openDuv(srcRows).use { src ->
            openDuv(destRows).use { perRow ->
                openDuv(destRows).use { batched ->
                    val perRowCopier = src.rowCopier(perRow)
                    sel.forEach { perRowCopier.copyRow(it) }

                    copy(src.rowCopier(batched))

                    batched.rows shouldBe perRow.rows
                }
            }
        }
    }

    @Test
    fun `copyRows reads back the same values as copyRow`() = runTest {
        checkAll(arbitrary {
            val srcRows = Arb.list(arbRow, 0..40).bind()
            val destRows = Arb.list(arbRow, 0..4).bind()
            val sel = if (srcRows.isEmpty()) IntArray(0) else Arb.list(Arb.int(srcRows.indices), 0..48).bind().toIntArray()
            val split = Arb.int(0..sel.size).bind()
            Triple(srcRows, destRows, sel to split)
        }) { (srcRows, destRows, selSplit) ->
            val (sel, split) = selSplit

            checkSameValuesAsCopyRow(srcRows, destRows, sel) { copier ->
                copier.copyRows(sel.copyOfRange(0, split))
                copier.copyRows(sel.copyOfRange(split, sel.size))
            }
        }
    }

    @Test
    fun `copyRange over part of the source reads back the same values as copyRow`() = runTest {
        checkAll(arbitrary {
            val srcRows = Arb.list(arbRow, 1..40).bind()
            val destRows = Arb.list(arbRow, 0..4).bind()
            val start = Arb.int(0..srcRows.size).bind()
            val len = Arb.int(0..srcRows.size - start - (if (start == 0) 1 else 0)).bind()
            Triple(srcRows, destRows, start to len)
        }) { (srcRows, destRows, range) ->
            val (start, len) = range

            checkSameValuesAsCopyRow(srcRows, destRows, IntArray(len) { start + it }) { copier ->
                copier.copyRange(start, len)
            }
        }
    }

    @Test
    fun `getAsLong and readAsLong read each row through its own integer leg`() {
        DenseUnionVector(
            allocator,
            listOf("i16", "i32", "i64"),
            listOf(
                ShortVector(allocator, false),
                IntVector.open(allocator, false),
                LongVector(allocator, false)
            )
        ).use { duv ->
            duv.vectorFor("i16").writeShort(-12)
            duv.vectorFor("i32").writeInt(Int.MIN_VALUE)
            duv.vectorFor("i64").writeLong(Long.MAX_VALUE)

            val expected = listOf(-12L, Int.MIN_VALUE.toLong(), Long.MAX_VALUE)

            List(3) { duv.getAsLong(it) } shouldBe expected

            val selected = duv.select(intArrayOf(2, 0))
            List(2) { selected.getAsLong(it) } shouldBe listOf(Long.MAX_VALUE, -12L)

            val rdr = duv.valueReader()
            List(3) { rdr.pos = it; rdr.readAsLong() } shouldBe expected
        }
    }

    @Test
    fun `getAsDouble and readAsDouble read each row through its own floating-point leg`() {
        DenseUnionVector(
            allocator,
            listOf("f32", "f64"),
            listOf(
                FloatVector(allocator, false),
                DoubleVector(allocator, false)
            )
        ).use { duv ->
            duv.vectorFor("f32").writeFloat(1.5f)
            duv.vectorFor("f64").writeDouble(-2.25)

            val expected = listOf(1.5, -2.25)

            List(2) { duv.getAsDouble(it) } shouldBe expected

            val selected = duv.select(intArrayOf(1, 0))
            List(2) { selected.getAsDouble(it) } shouldBe expected.reversed()

            val rdr = duv.valueReader()
            List(2) { rdr.pos = it; rdr.readAsDouble() } shouldBe expected
        }
    }
}
