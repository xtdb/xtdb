package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.listy
import xtdb.arrow.VectorType.Companion.mapTypeOf
import xtdb.test.AllocatorResolver
import xtdb.time.Interval
import xtdb.types.ZonedDateTimeRange
import java.math.BigDecimal
import java.net.URI
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.UUID

@ExtendWith(AllocatorResolver::class)
class VectorValueReaderTest {

    private fun zdt(sec: Long) = ZonedDateTime.ofInstant(Instant.ofEpochSecond(sec), ZoneId.of("UTC"))

    private val vectors: Map<String, (BufferAllocator) -> VectorReader> = mapOf(
        "i64" to { al -> Vector.fromList(al, listOf(1L, 2L, 3L, 4L, 5L)) },
        "nullable utf8" to { al -> Vector.fromList(al, listOf("a", null, "c", "d", null)) },
        "f64" to { al -> Vector.fromList(al, listOf(1.5, 2.5, 3.5, 4.5, 5.5)) },
        "bool" to { al -> Vector.fromList(al, listOf(true, false, true, true, false)) },
        "timestamp" to { al -> Vector.fromList(al, List(5) { Instant.ofEpochSecond(it.toLong()) }) },
        "duration" to { al -> Vector.fromList(al, List(5) { Duration.ofSeconds(it.toLong()) }) },
        "uuid" to { al -> Vector.fromList(al, List(5) { UUID(0, it.toLong()) }) },
        "uri" to { al -> Vector.fromList(al, List(5) { URI("https://example.com/$it") }) },
        "interval" to { al -> Vector.fromList(al, List(5) { Interval(it, it + 1, it * 1_000L) }) },
        "decimal" to { al -> Vector.fromList(al, List(5) { BigDecimal("$it.25") }) },
        "list" to { al -> Vector.fromList(al, List(5) { listOf(it, it + 1) }) },
        "set" to { al -> Vector.fromList(al, List(5) { setOf(it, it + 10) }) },
        "fixed-size list" to { al ->
            Vector.fromList(al, listy(ArrowType.FixedSizeList(2), I64), List(5) { listOf(it.toLong(), it + 1L) })
        },
        "map" to { al ->
            Vector.fromList(al, mapTypeOf(I64, UTF8), List(5) { mapOf(it.toLong() to "v$it") })
        },
        "struct" to { al -> Vector.fromList(al, List(5) { mapOf("fooBar" to it, "baz" to "s$it") }) },
        "union" to { al -> Vector.fromList(al, listOf(1L, "two", 3.0, null, listOf(5L))) },
        "tstz-range" to { al ->
            Vector.fromList(al, List(5) { ZonedDateTimeRange(zdt(it.toLong()), if (it % 2 == 0) null else zdt(it + 10L)) })
        },
        "run-end encoded" to { al ->
            RunEndEncodedVector(al, Utf8Vector(al, true))
                .also { it.writeRun("a", 2); it.writeRun(null, 1); it.writeRun("b", 2) }
        },
        "struct opened from a relation" to { al ->
            Relation.openFromRows(al, List(5) { mapOf("fooBar" to it.toLong()) }).use { it.openAsStruct(al) }
        },
    )

    private fun checkAgainstGetObject(rdr: VectorReader, what: String) {
        assertEquals(
            List(rdr.valueCount) { rdr.getObject(it, SNAKE_CASE_STRING) },
            rdr.valueReader().asSequence().map { it.readObject(SNAKE_CASE_STRING) }.toList(),
            "$what, iterated"
        )

        val seeked = rdr.valueReader()
        assertEquals(
            List(rdr.valueCount) { rdr.getObject(it) },
            List(rdr.valueCount) { seeked.pos = it; seeked.readObject() },
            "$what, seeked"
        )
    }

    private fun checkSelections(vec: VectorReader, label: String) {
        checkAgainstGetObject(vec.select(intArrayOf(4, 0, 2, 2)), "$label, select(IntArray)")
        checkAgainstGetObject(vec.select(intArrayOf(4, 0, 2)).select(intArrayOf(2, 1)), "$label, nested select")
    }

    @Test
    fun `a value reader reads each row as getObject does, iterated and seeked, directly and through selections`(al: BufferAllocator) {
        vectors.forEach { (label, open) ->
            open(al).use { vec ->
                checkAgainstGetObject(vec, "$label, direct")
                checkSelections(vec, label)
            }
        }
    }

    @Test
    fun `a value reader carries on iterating from wherever it was positioned`(al: BufferAllocator) {
        vectors.forEach { (label, open) ->
            open(al).use { vec ->
                fun check(rdr: VectorReader, how: String) {
                    val row = rdr.valueReader()
                    row.pos = 1
                    assertEquals(rdr.getObject(1, SNAKE_CASE_STRING), row.readObject(SNAKE_CASE_STRING), "$label, $how, seek")
                    assertEquals(
                        List(rdr.valueCount - 2) { rdr.getObject(it + 2, SNAKE_CASE_STRING) },
                        row.asSequence().map { it.readObject(SNAKE_CASE_STRING) }.toList(),
                        "$label, $how, iterated on"
                    )
                }

                check(vec, "direct")
                check(vec.select(intArrayOf(4, 0, 2, 2)), "select(IntArray)")
            }
        }
    }

    @Test
    fun `an iteration visits the rows present when the reader was made, and a seek reaches rows appended since`(al: BufferAllocator) {
        vectors.forEach { (label, open) ->
            open(al).use { vec ->
                if (vec !is Vector) return@use

                val rows = vec.valueReader()
                rows.next()
                vec.writeObject(vec.getObject(0))

                assertEquals(
                    List(4) { vec.getObject(it + 1, SNAKE_CASE_STRING) },
                    rows.asSequence().map { it.readObject(SNAKE_CASE_STRING) }.toList(),
                    "$label, iterated"
                )

                rows.pos = 5
                assertEquals(vec.getObject(5, SNAKE_CASE_STRING), rows.readObject(SNAKE_CASE_STRING), "$label, seek")
            }
        }
    }

    @Test
    fun `a dense union's leg reads through its value reader, directly and through selections`(al: BufferAllocator) {
        Vector.fromList(al, listOf(1L, "a", 2L, "b", 3L)).use { union ->
            val leg = union.vectorFor("i64")
            checkAgainstGetObject(leg, "i64 leg, direct")
            checkSelections(leg, "i64 leg")
        }
    }

    @Test
    fun `an empty vector has no rows`(al: BufferAllocator) {
        Vector.fromList(al, listOf<Long>()).use { vec ->
            val rows = vec.valueReader()
            assertFalse(rows.hasNext())
            assertThrows<NoSuchElementException> { rows.next() }
        }
    }

    @Test
    fun `a struct opened from a relation reads each row's children, directly and through selections`(al: BufferAllocator) {
        val rows = listOf(mapOf("a" to 0L, "b" to "x"), mapOf("a" to 1L, "b" to null), mapOf("a" to 2L, "b" to "z"))

        Relation.openFromRows(al, rows).use { it.openAsStruct(al) }.use { vec ->
            fun check(rdr: VectorReader, expected: List<Map<String, Any?>>, how: String) {
                val actual = rdr.valueReader().asSequence()
                    .map { row -> assertFalse(row.isNull, how); row.snapshot() }
                    .toList()
                assertEquals(expected, actual, how)
            }

            check(vec, rows, "direct")
            check(vec.select(intArrayOf(2, 0, 0)), listOf(rows[2], rows[0], rows[0]), "select(IntArray)")
        }
    }

    @Test
    fun `a one-row list of structs reads through its value reader, directly and through selections`(al: BufferAllocator) {
        val docs = List(3) { mapOf("fooBar" to it.toLong()) }
        Vector.fromList(al, listOf(docs)).use { vec ->
            checkAgainstGetObject(vec, "direct")
            checkAgainstGetObject(vec.select(intArrayOf(0, 0)), "select(IntArray)")

            assertEquals(listOf(docs), vec.valueReader().asSequence().map { it.snapshot() }.toList())
        }
    }

    private fun outcome(read: () -> Any?): Any? = runCatching(read).fold({ it }, { it::class })

    private fun ValueReader.snapshot(): Any? = when {
        isNull -> null
        else -> runCatching { readList().let { l -> List(l.size()) { l.nth(it).snapshot() } } }
            .recoverCatching { readStruct().mapValues { (_, v) -> v.snapshot() } }
            .getOrElse { readObject() }
    }

    @Test
    fun `an iterated value reader reads as one positioned at each row, and as the vector, directly and through a selection`(al: BufferAllocator) {
        val shapes = mapOf(
            "list" to listOf(listOf(1L, 2L), null, listOf(3L), listOf(), listOf(4L, 5L)),
            "struct" to listOf(mapOf("a" to 1L, "b" to "x"), null, mapOf("a" to 2L, "b" to "y"), mapOf("a" to 3L, "b" to null), mapOf("a" to 4L, "b" to "z")),
            "union" to listOf(1L, "two", null, listOf(4L), 5L),
            "interval" to List(5) { Interval(it, it + 1, it * 1_000L) },
            "integer union" to listOf((-12).toShort(), Int.MIN_VALUE, null, Long.MAX_VALUE, 7.toShort()),
            "floating-point union" to listOf(1.5f, -2.25, null, Double.MAX_VALUE, Float.MIN_VALUE),
        )

        shapes.forEach { (label, values) ->
            Vector.fromList(al, values).use { vec ->
                fun check(rdr: VectorReader, how: String) {
                    val expected = rdr.valueReader()
                    rdr.valueReader().forEach { row ->
                        expected.pos = row.pos
                        val what = "$label, $how, row ${row.pos}"
                        assertEquals(expected.isNull, row.isNull, what)
                        assertEquals(expected.snapshot(), row.snapshot(), what)
                        assertEquals(outcome { rdr.getObject(row.pos) }, outcome { row.readObject() }, what)
                        if (label == "union") assertEquals(expected.leg, row.leg, what)
                        if (!row.isNull) {
                            assertEquals(outcome { rdr.getAsLong(row.pos) }, outcome { row.readAsLong() }, what)
                            assertEquals(outcome { rdr.getAsDouble(row.pos) }, outcome { row.readAsDouble() }, what)
                        }
                    }
                }

                check(vec, "direct")
                check(vec.select(intArrayOf(4, 0, 2, 1)), "select(IntArray)")
            }
        }

        Vector.fromList(al, listOf((-12).toShort(), Int.MIN_VALUE, null, Long.MAX_VALUE)).use { vec ->
            assertEquals(
                listOf(-12L, Int.MIN_VALUE.toLong(), null, Long.MAX_VALUE),
                vec.valueReader().asSequence().map { if (it.isNull) null else it.readAsLong() }.toList()
            )
        }

        Vector.fromList(al, listOf(1.5f, null, -2.25)).use { vec ->
            assertEquals(
                listOf(1.5, null, -2.25),
                vec.select(intArrayOf(0, 1, 2)).valueReader().asSequence().map { if (it.isNull) null else it.readAsDouble() }.toList()
            )
        }

        Vector.fromList(al, listOf(10L, null, 30L)).use { vec ->
            val rows = vec.select(intArrayOf(2, 1, 0)).valueReader()
            assertEquals(30L, rows.next().readLong())
            assertEquals(true, rows.next().isNull)
            assertEquals(10L, rows.next().readLong())
        }
    }

    @Test
    fun `a value reader through a selection is one reader, on the logical row`(al: BufferAllocator) {
        Vector.fromList(al, listOf(10L, 20L, 30L, 40L)).use { vec ->
            val rows = vec.select(intArrayOf(3, 1)).valueReader()

            val first = rows.next()
            assertEquals(0 to 40L, first.pos to first.readLong())

            val second = rows.next()
            assertSame(first, second)
            assertEquals(1 to 20L, second.pos to second.readLong())

            assertFalse(rows.hasNext())
            assertThrows<NoSuchElementException> { rows.next() }
        }
    }
}
