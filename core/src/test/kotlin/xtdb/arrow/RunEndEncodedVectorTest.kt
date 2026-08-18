package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.arrow.Relation.Companion.loader
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.asType
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.bloom.BloomBuilder
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.Random
import org.apache.arrow.vector.BigIntVector as ArrowBigIntVector
import org.apache.arrow.vector.IntVector as ArrowIntVector
import org.apache.arrow.vector.complex.RunEndEncodedVector as ArrowRunEndEncodedVector

class RunEndEncodedVectorTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    /** Encodes `runs` — (value, run-length) pairs — as an i64 column. */
    private fun openI64Runs(vararg runs: Pair<Long?, Int>) =
        RunEndEncodedVector(allocator, "vals", LongVector(allocator, REE_VALUES_NAME, true))
            .also { vec -> runs.forEach { (value, len) -> vec.writeRun(value, len) } }

    private fun expand(vararg runs: Pair<Long?, Int>) = runs.flatMap { (value, len) -> List(len) { value } }

    @Test
    fun `reads back the rows it encodes`() {
        val runs = arrayOf(10L to 3, 20L to 1, 30L to 4)

        openI64Runs(*runs).use { vec ->
            assertEquals(8, vec.valueCount)
            assertEquals(expand(*runs), vec.asList)
            assertEquals(listOf(10L, 10L, 10L, 20L, 30L, 30L, 30L, 30L), (0..<vec.valueCount).map { vec.getLong(it) })
        }
    }

    @Test
    fun `nulls live in the values child`() {
        val runs = arrayOf(10L to 2, null to 3, 20L to 1)

        openI64Runs(*runs).use { vec ->
            assertEquals(expand(*runs), vec.asList)
            assertEquals(
                listOf(false, false, true, true, true, false),
                (0..<vec.valueCount).map { vec.isNull(it) }
            )
        }
    }

    @Test
    fun `exposes the runs themselves`() {
        openI64Runs(10L to 3, 20L to 1, 30L to 4).use { vec ->
            assertEquals(3, vec.runCount)

            assertEquals(listOf(0, 3, 4), (0..<vec.runCount).map { vec.runStart(it) })
            assertEquals(listOf(3, 4, 8), (0..<vec.runCount).map { vec.runEnd(it) })

            assertEquals(listOf(10L, 20L, 30L), vec.runValues.asList)
            assertEquals(listOf(3, 4, 8), vec.runEnds.asList)
        }
    }

    @Test
    fun `finds the run for an index from any starting point`() {
        val runLengths = listOf(1, 4, 1, 1, 7, 2, 1, 3, 1, 1)
        val runs = runLengths.mapIndexed { i, len -> (i * 10L) to len }.toTypedArray()

        openI64Runs(*runs).use { vec ->
            val expected = runLengths.flatMapIndexed { runIdx, len -> List(len) { runIdx } }
            assertEquals(vec.valueCount, expected.size)

            val ascending = (0..<vec.valueCount).toList()

            for (order in listOf(ascending, ascending.reversed(), ascending.shuffled(Random(0)))) {
                assertEquals(order.map { expected[it] }, order.map { vec.runIdx(it) }, "order: $order")
            }
        }
    }

    @Test
    fun `an iid reads as a pair of longs`() {
        val iids = listOf(
            byteArrayOf(0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2),
            byteArrayOf(0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4)
        )

        openIidRuns(iids[0] to 3, iids[1] to 2).use { vec ->
            openIidPlain(iids[0], iids[0], iids[0], iids[1], iids[1]).use { plain ->
                assertEquals(
                    (0..<plain.valueCount).map { plain.getLongLongHigh(it) to plain.getLongLongLow(it) },
                    (0..<vec.valueCount).map { vec.getLongLongHigh(it) to vec.getLongLongLow(it) }
                )
            }
        }
    }

    @Test
    fun `the field carries the encoding, the type carries the values`() {
        openI64Runs(10L to 3).use { vec ->
            assertEquals(RUN_END_ENCODED_TYPE, vec.arrowType)
            assertEquals(listOf(REE_RUN_ENDS_NAME, REE_VALUES_NAME), vec.field.children.map { it.name })

            assertEquals(VectorType.maybe(VectorType.I64), vec.type)
            assertEquals(vec.type, vec.field.asType)
        }
    }

    @Test
    fun `copies out as the rows it encodes`() {
        openI64Runs(10L to 3, 20L to 1, 30L to 2).use { vec ->
            LongVector(allocator, "out", true).use { out ->
                out.append(vec)
                assertEquals(vec.asList, out.asList)
            }

            LongVector(allocator, "out", true).use { out ->
                out.appendRange(vec, 2, 3)
                assertEquals(listOf(10L, 20L, 30L), out.asList)
            }

            LongVector(allocator, "out", true).use { out ->
                out.appendRows(vec, intArrayOf(5, 0, 3))
                assertEquals(listOf(30L, 10L, 20L), out.asList)
            }
        }
    }

    @Test
    fun `copies into an encoded column a run at a time`() {
        openI64Runs(10L to 3, 20L to 1, 30L to 2).use { vec ->
            RunEndEncodedVector(allocator, "out", LongVector(allocator, REE_VALUES_NAME, true)).use { out ->
                out.append(vec)

                assertEquals(vec.asList, out.asList)
                assertEquals(vec.runCount, out.runCount, "a whole-range copy keeps the runs")
            }

            RunEndEncodedVector(allocator, "out", LongVector(allocator, REE_VALUES_NAME, true)).use { out ->
                out.appendRange(vec, 2, 3)

                assertEquals(listOf(10L, 20L, 30L), out.asList)
                assertEquals(3, out.runCount, "a range straddling run boundaries splits them")
            }
        }
    }

    @Test
    fun `a selection materialises back to an encoded column`() {
        openI64Runs(10L to 3, 20L to 1, 30L to 2).use { vec ->
            vec.select(intArrayOf(0, 1, 4)).openDirectSlice(allocator).use { out ->
                assertTrue(out is RunEndEncodedVector, "keeps the encoding: $out")
                assertEquals(listOf(10L, 10L, 30L), out.asList)
            }
        }
    }

    @Test
    fun `slices and selections read logically`() {
        openI64Runs(10L to 3, 20L to 1, 30L to 2).use { vec ->
            vec.openSlice(allocator).use { slice ->
                assertEquals(vec.asList, slice.asList)
                assertEquals(3, slice.runCount)
            }

            assertEquals(listOf(20L, 30L), vec.select(3, 2).asList)
            assertEquals(listOf(30L, 10L), vec.select(intArrayOf(4, 1)).asList)
        }
    }

    @Test
    fun `runs summarise to the same metadata as the rows`() {
        val runs = arrayOf(10L to 3, 20L to 1, 30L to 4)

        openI64Runs(*runs).use { vec ->
            LongVector(allocator, "vals", true).use { plain ->
                expand(*runs).forEach { plain.writeObject(it) }

                assertEquals(bloomOf(plain), bloomOf(vec))

                val flavours = vec.metadataFlavours.filterIsInstance<MetadataFlavour.Number>()
                assertEquals(10.0, flavours.minOf { f -> (0..<f.valueCount).minOf { f.getMetaDouble(it) } })
                assertEquals(30.0, flavours.maxOf { f -> (0..<f.valueCount).maxOf { f.getMetaDouble(it) } })
            }
        }
    }

    @Test
    fun `round-trips through Arrow IPC`() {
        val runs = arrayOf(10L to 3, null to 2, 30L to 4)
        val bytes = ByteArrayOutputStream()

        openI64Runs(*runs).use { vec ->
            val rel = Relation(allocator, listOf(vec), vec.valueCount)
            rel.startUnload(Channels.newChannel(bytes)).use { unloader ->
                unloader.writePage()
                unloader.end()
            }
        }

        loader(allocator, bytes.toByteArray()).use { loader ->
            Relation(allocator, loader.schema).use { rel ->
                loader.loadPage(0, rel)

                val vec = rel["vals"]
                assertTrue(vec is RunEndEncodedVector, "reads back run-encoded, not decoded: $vec")
                assertEquals(3, (vec as RunEndEncodedVector).runCount)

                assertEquals(9, rel.rowCount)
                assertEquals(expand(*runs), vec.asList)
            }
        }
    }

    @Test
    fun `reads run-ends of any width`() {
        for (runEnds in listOf(
            ShortVector(allocator, REE_RUN_ENDS_NAME, false),
            IntVector.open(allocator, REE_RUN_ENDS_NAME, false),
            LongVector(allocator, REE_RUN_ENDS_NAME, false)
        )) {
            RunEndEncodedVector("vals", runEnds, LongVector(allocator, REE_VALUES_NAME, true)).use { vec ->
                vec.writeRun(10L, 3)
                vec.writeRun(20L, 2)

                assertEquals(listOf(10L, 10L, 10L, 20L, 20L), vec.asList, "run-ends: ${runEnds.arrowType}")
            }
        }
    }

    @Test
    fun `reads a run-encoded column nested in a struct`() {
        val reeField = Field(
            "ree", FieldType.notNullable(RUN_END_ENCODED_TYPE),
            listOf(
                Field(REE_RUN_ENDS_NAME, FieldType.notNullable(I32_TYPE), null),
                Field(REE_VALUES_NAME, FieldType.nullable(I64_TYPE), null)
            )
        )

        val structField = Field("outer", FieldType.notNullable(STRUCT_TYPE), listOf(reeField))

        allocator.openVector(structField).use { structVec ->
            val ree = structVec.vectorFor("ree") as RunEndEncodedVector
            ree.writeRun(10L, 2)
            ree.writeRun(20L, 1)

            assertEquals(listOf(10L, 10L, 20L), ree.asList)
            assertEquals(structField, structVec.field)
        }
    }

    @Test
    fun `reads struct values whole, without offering up their children`() {
        val reeField = Field(
            "ree", FieldType.notNullable(RUN_END_ENCODED_TYPE),
            listOf(
                Field(REE_RUN_ENDS_NAME, FieldType.notNullable(I32_TYPE), null),
                Field(
                    REE_VALUES_NAME, FieldType.nullable(STRUCT_TYPE),
                    listOf(Field("a", FieldType.nullable(I64_TYPE), null))
                )
            )
        )

        allocator.openVector(reeField).use { vec ->
            vec as RunEndEncodedVector

            vec.writeRun(mapOf("a" to 1L), 2)
            vec.writeRun(mapOf("a" to 2L), 1)

            assertEquals(listOf(1L, 1L, 2L), (0..<vec.valueCount).map { (vec.getObject(it) as Map<*, *>)["a"] })

            assertNull(vec.keyNames)
            assertThrows<UnsupportedOperationException> { vec.vectorForOrNull("a") }
        }
    }

    /** Our own writer maintains these invariants, so a page that breaks one has to be built through Arrow's. */
    private fun openArrowRuns(runEnds: List<Int>, values: List<Long>, valueCount: Int): ArrowRunEndEncodedVector {
        val runEndsField = Field(REE_RUN_ENDS_NAME, FieldType.notNullable(I32_TYPE), null)
        val valuesField = Field(REE_VALUES_NAME, FieldType.nullable(I64_TYPE), null)
        val reeField = Field(
            "vals", FieldType.notNullable(RUN_END_ENCODED_TYPE), listOf(runEndsField, valuesField)
        )

        return ArrowRunEndEncodedVector(reeField, allocator, null).also { arrowVec ->
            arrowVec.initializeChildrenFromFields(listOf(runEndsField, valuesField))

            (arrowVec.runEndsVector as ArrowIntVector).apply {
                runEnds.forEachIndexed { idx, runEnd -> setSafe(idx, runEnd) }
                this.valueCount = runEnds.size
            }

            (arrowVec.valuesVector as ArrowBigIntVector).apply {
                values.forEachIndexed { idx, value -> setSafe(idx, value) }
                this.valueCount = values.size
            }

            arrowVec.valueCount = valueCount
        }
    }

    @Test
    fun `refuses a page whose runs stop short of its rows`() {
        openArrowRuns(runEnds = listOf(2), values = listOf(10L), valueCount = 5)
            .use { assertThrows<IllegalStateException> { Vector.fromArrow(it).close() } }
    }

    @Test
    fun `refuses a page with a run it has no value for`() {
        openArrowRuns(runEnds = listOf(2, 4), values = listOf(10L), valueCount = 4)
            .use { assertThrows<IllegalStateException> { Vector.fromArrow(it).close() } }
    }

    @Test
    fun `refuses run-ends that don't ascend`() {
        openArrowRuns(runEnds = listOf(4, 2, 6), values = listOf(10L, 20L, 30L), valueCount = 6)
            .use { assertThrows<IllegalStateException> { Vector.fromArrow(it).close() } }
    }

    @Test
    fun `refuses more rows than its run-ends can count`() {
        RunEndEncodedVector(
            "vals", ShortVector(allocator, REE_RUN_ENDS_NAME, false), LongVector(allocator, REE_VALUES_NAME, true)
        ).use { vec ->
            assertThrows<IllegalArgumentException> { vec.writeRun(10L, Short.MAX_VALUE + 1) }
            assertEquals(0, vec.valueCount, "a rejected run doesn't count towards the rows")
        }
    }

    @Test
    fun `refuses a run-encoded field without both children`() {
        val field = Field(
            "vals", FieldType.notNullable(RUN_END_ENCODED_TYPE),
            listOf(Field(REE_VALUES_NAME, FieldType.nullable(I64_TYPE), null))
        )

        assertThrows<IllegalArgumentException> { field.asType }
        assertThrows<IllegalArgumentException> { allocator.openVector(field).close() }
    }

    @Test
    fun `reads a file written by Arrow's own writer`() {
        val bytes = ByteArrayOutputStream()

        val runEndsField = Field(REE_RUN_ENDS_NAME, FieldType.notNullable(I32_TYPE), null)
        val valuesField = Field(REE_VALUES_NAME, FieldType.nullable(I64_TYPE), null)
        val reeField = Field(
            "vals", FieldType.notNullable(RUN_END_ENCODED_TYPE), listOf(runEndsField, valuesField)
        )

        ArrowRunEndEncodedVector(reeField, allocator, null).use { arrowVec ->
            arrowVec.initializeChildrenFromFields(listOf(runEndsField, valuesField))

            val runEnds = arrowVec.runEndsVector as ArrowIntVector
            val values = arrowVec.valuesVector as ArrowBigIntVector

            runEnds.allocateNew(3)
            values.allocateNew(3)

            listOf(3 to 10L, 4 to null, 8 to 30L).forEachIndexed { runIdx, (runEnd, value) ->
                runEnds.setSafe(runIdx, runEnd)
                if (value == null) values.setNull(runIdx) else values.setSafe(runIdx, value)
            }

            runEnds.valueCount = 3
            values.valueCount = 3
            arrowVec.valueCount = 8

            VectorSchemaRoot(listOf(reeField), listOf(arrowVec), 8).use { root ->
                ArrowFileWriter(root, null, Channels.newChannel(bytes)).use { writer ->
                    writer.start()
                    writer.writeBatch()
                    writer.end()
                }
            }
        }

        loader(allocator, bytes.toByteArray()).use { loader ->
            Relation(allocator, loader.schema).use { rel ->
                loader.loadPage(0, rel)

                assertEquals(8, rel.rowCount)
                assertEquals(
                    listOf(10L, 10L, 10L, null, 30L, 30L, 30L, 30L),
                    rel["vals"].asList
                )
            }
        }
    }

    @Test
    fun `loads from an Arrow vector in memory`() {
        val runEndsField = Field(REE_RUN_ENDS_NAME, FieldType.notNullable(I32_TYPE), null)
        val valuesField = Field(REE_VALUES_NAME, FieldType.nullable(I64_TYPE), null)
        val reeField = Field(
            "vals", FieldType.notNullable(RUN_END_ENCODED_TYPE), listOf(runEndsField, valuesField)
        )

        ArrowRunEndEncodedVector(reeField, allocator, null).use { arrowVec ->
            arrowVec.initializeChildrenFromFields(listOf(runEndsField, valuesField))

            val runEnds = arrowVec.runEndsVector as ArrowIntVector
            val values = arrowVec.valuesVector as ArrowBigIntVector

            runEnds.setSafe(0, 2); values.setSafe(0, 10L)
            runEnds.setSafe(1, 5); values.setSafe(1, 20L)
            runEnds.valueCount = 2
            values.valueCount = 2
            arrowVec.valueCount = 5

            Vector.fromArrow(arrowVec).use { vec ->
                assertTrue(vec is RunEndEncodedVector, "loads as run-encoded: $vec")
                assertEquals(listOf(10L, 10L, 20L, 20L, 20L), vec.asList)
                assertFalse(vec.isNull(0))
            }
        }
    }

    @Test
    fun `a typed write is a run of one row`() {
        val iids = listOf(
            byteArrayOf(0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2),
            byteArrayOf(0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4)
        )

        RunEndEncodedVector(allocator, "_iid", FixedSizeBinaryVector(allocator, REE_VALUES_NAME, false, 16))
            .use { vec ->
                listOf(iids[0], iids[0], iids[1]).forEach { vec.writeBytes(it) }

                assertEquals(3, vec.valueCount)
                assertEquals(3, vec.runCount, "equal neighbours are not coalesced")

                openIidPlain(iids[0], iids[0], iids[1]).use { plain ->
                    assertEquals(
                        (0..<plain.valueCount).map { plain.getLongLongHigh(it) to plain.getLongLongLow(it) },
                        (0..<vec.valueCount).map { vec.getLongLongHigh(it) to vec.getLongLongLow(it) }
                    )
                }
            }

        RunEndEncodedVector(allocator, "vals", LongVector(allocator, REE_VALUES_NAME, false)).use { vec ->
            listOf(10L, 10L, 20L).forEach { vec.writeLong(it) }

            assertEquals(listOf(10L, 10L, 20L), vec.asList)
            assertEquals(3, vec.runCount)
        }
    }

    private fun openIidRuns(vararg runs: Pair<ByteArray, Int>) =
        RunEndEncodedVector(allocator, "_iid", FixedSizeBinaryVector(allocator, REE_VALUES_NAME, false, 16))
            .also { vec -> runs.forEach { (iid, len) -> vec.writeRun(iid, len) } }

    private fun openIidPlain(vararg iids: ByteArray) =
        FixedSizeBinaryVector(allocator, "_iid", false, 16).also { vec -> iids.forEach { vec.writeObject(it) } }

    private fun bloomOf(vec: VectorReader) =
        BloomBuilder().also { builder ->
            vec.metadataFlavours.forEach { flavour ->
                repeat(flavour.valueCount) { builder.add(flavour as VectorReader, it) }
            }
        }.build()
}
