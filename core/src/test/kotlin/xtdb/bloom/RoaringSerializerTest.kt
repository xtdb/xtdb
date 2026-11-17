package xtdb.bloom

import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import java.util.Random
import kotlin.random.asKotlinRandom

class RoaringSerializerTest {

    private fun assertRoundTrips(values: IntArray, n: Int = values.size) {
        val expected = values.copyOf(n).distinct().sortedBy { it.toUInt() }.toIntArray()

        val written = RoaringSerializer.serialize(values, n)

        val immutable = ImmutableRoaringBitmap(written.duplicate())
        immutable.serializedSizeInBytes() shouldBe written.remaining()
        immutable.cardinality shouldBe expected.size
        immutable.toArray() shouldBe expected

        val deserialised = RoaringBitmap().apply { deserialize(written.duplicate()) }
        deserialised.cardinality shouldBe expected.size
        deserialised.toArray() shouldBe expected
    }

    private sealed interface Shape {
        fun lows(): IntArray

        data class Sparse(val lows: Set<Int>) : Shape {
            override fun lows() = lows.toIntArray()
        }

        data class Runs(val first: Int, val length: Int, val gap: Int, val count: Int) : Shape {
            override fun lows() =
                (0 until count).flatMap { r -> (0 until length).map { first + r * (length + gap) + it } }
                    .filter { it < 65536 }.toIntArray()
        }

        data class Distinct(val cardinality: Int, val seed: Long) : Shape {
            override fun lows() =
                IntArray(65536) { it }.also { it.shuffle(Random(seed).asKotlinRandom()) }.copyOf(cardinality)
        }

        data class RunArrayTie(val runs: Int, val cardinalityOverTie: Int, val gap: Int, val seed: Long) : Shape {
            override fun lows(): IntArray {
                val rnd = Random(seed).asKotlinRandom()
                val cardinality = 1 + 2 * runs + cardinalityOverTie
                val lengths = IntArray(runs) { 1 }
                repeat(cardinality - runs) { lengths[rnd.nextInt(runs)]++ }
                var low = 0
                return lengths.flatMap { len -> (low until low + len).also { low += len + gap } }.toIntArray()
            }
        }

        data object Full : Shape {
            override fun lows() = IntArray(65536) { it }
        }
    }

    private data class Case(val containers: Map<Int, Shape>, val duplicates: Int, val shuffleSeed: Long, val prefix: Int?) {
        fun values(): IntArray {
            val distinct = containers.flatMap { (key, shape) -> shape.lows().map { (key shl 16) or it } }
            return (distinct + List(if (distinct.isEmpty()) 0 else duplicates) { distinct[it % distinct.size] })
                .shuffled(Random(shuffleSeed).asKotlinRandom()).toIntArray()
        }
    }

    private val arbShape: Arb<Shape> = Arb.choose(
        3 to arbitrary { Shape.Sparse(Arb.set(Arb.int(0..65535), 1..200).bind()) },
        3 to arbitrary {
            Shape.Runs(Arb.int(0..1000).bind(), Arb.int(1..64).bind(), Arb.int(1..64).bind(), Arb.int(1..1000).bind())
        },
        2 to arbitrary { Shape.Distinct(Arb.int(4094..4098).bind(), Arb.long().bind()) },
        3 to arbitrary {
            Shape.RunArrayTie(Arb.int(1..2047).bind(), Arb.int(-1..1).bind(), Arb.int(1..8).bind(), Arb.long().bind())
        },
        1 to Arb.constant(Shape.Full),
    )

    private val arbCase = arbitrary {
        val keys = Arb.set(Arb.int(0..65535), 0..20).bind()
        val values = Case(keys.associateWith { arbShape.bind() }, Arb.int(0..100).bind(), Arb.long().bind(), null)
        val size = values.values().size
        values.copy(prefix = Arb.choice(Arb.constant(null), Arb.int(0..size)).bind())
    }

    @Test
    fun `generated sets of every container shape read back through Roaring as the same set`() = runTest {
        checkAll(PropTestConfig(iterations = 200), arbCase) { case ->
            val values = case.values()
            assertRoundTrips(values, case.prefix ?: values.size)
        }
    }

    @Test
    fun `random sets of bloom positions read back through Roaring as the same set`() {
        val rnd = Random(42)
        for (n in listOf(0, 1, 5, 100, 5_000, 40_000, 300_000, 1_000_000, 4_000_000)) {
            assertRoundTrips(IntArray(n) { rnd.nextInt(1 shl BLOOM_BITS) })
        }
    }

    @Test
    fun `array, bitmap and run containers each read back through Roaring as the same set`() {
        val rnd = Random(7)

        assertRoundTrips(IntArray(20_000) { rnd.nextInt(1 shl 16) })
        assertRoundTrips(IntArray(5 * 65536) { it })
        assertRoundTrips(IntArray(65536) { it + 3 * 65536 })
        assertRoundTrips(IntArray(3000) { (it / 10) * 100 + it % 10 })

        for (containers in 1..8) {
            assertRoundTrips(IntArray(containers * 6000) { (it / 6000) * 65536 + it % 6000 })
        }

        for (containers in 1..8) {
            assertRoundTrips(IntArray(containers * 6000) { (it / 6000) * 65536 + rnd.nextInt(65536) })
        }
    }

    @Test
    fun `duplicates, disorder and a short prefix read back as the distinct values of the prefix`() {
        val rnd = Random(3)

        assertRoundTrips(IntArray(10_000) { rnd.nextInt(50) }.also { it.reverse() })
        assertRoundTrips(IntArray(10_000) { rnd.nextInt(1 shl BLOOM_BITS) }, 1_234)
    }

    @Test
    fun `negative ints sort after positive ones, as Roaring's unsigned keys do`() {
        val rnd = Random(11)

        assertRoundTrips(IntArray(50_000) { rnd.nextInt() })
        assertRoundTrips(intArrayOf(-1, 0, Int.MAX_VALUE, Int.MIN_VALUE, -2, 1))
    }
}
