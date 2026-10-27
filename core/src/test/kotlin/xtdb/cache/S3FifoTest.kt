package xtdb.cache

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random

class S3FifoTest {

    private class TestEntry(override val key: Int, override val bytes: Long = 1) : S3Fifo.Entry<Int> {
        override var freq = 0
        override var isHeld = false
    }

    private class Reader(private val maxBytes: Long, detectLoops: Boolean = true) {
        val policy = S3Fifo<Int, TestEntry>(maxBytes, detectLoops)
        val resident = mutableMapOf<Int, TestEntry>()

        fun read(key: Int): Boolean {
            resident[key]?.let { entry ->
                policy.hit(entry)
                return true
            }

            while (resident.size >= maxBytes) {
                val evicted = checkNotNull(policy.evict()) { "nothing evictable" }
                assertFalse(evicted.isHeld, "evicted held entry ${evicted.key}")
                resident.remove(evicted.key)
            }

            val entry = TestEntry(key)
            policy.admit(entry)
            resident[key] = entry
            return false
        }

        fun hitRate(keys: Iterable<Int>) = keys.count { read(it) }.toDouble() / keys.count()
    }

    @Test
    fun `a loop over more than the cache holds keeps a stable part of it resident`() {
        val reader = Reader(100)
        val loop = 0 until 125

        repeat(10) { reader.hitRate(loop) }

        val hitRate = (0 until 10).map { reader.hitRate(loop) }.average()
        assertTrue(hitRate > 0.6, "hit rate $hitRate")
    }

    @Test
    fun `a loop that moves on to other pages has them resident in its turn`() {
        val reader = Reader(100)

        repeat(10) { reader.hitRate(0 until 125) }

        val next = 1000 until 1125
        repeat(10) { reader.hitRate(next) }

        val hitRate = (0 until 10).map { reader.hitRate(next) }.average()
        assertTrue(hitRate > 0.6, "hit rate $hitRate")
    }

    @Test
    fun `a hot set that fits keeps hitting alongside pages read once`() {
        val reader = Reader(100)
        val hot = 0 until 50
        var oneShot = 1000

        fun pass() = hot.count { key -> reader.read(oneShot++); reader.read(key) }.toDouble() / hot.count()

        repeat(5) { pass() }

        val hitRate = (0 until 10).map { pass() }.average()
        assertTrue(hitRate > 0.95, "hit rate $hitRate")
        assertFalse(reader.policy.isFrozen)
    }

    private class Zipf(n: Int, seed: Long) {
        private val rand = Random(seed)
        private val cumulative = DoubleArray(n).also { c ->
            var sum = 0.0
            for (i in 0 until n) { sum += 1.0 / (i + 1); c[i] = sum }
        }

        fun next(): Int {
            val idx = cumulative.binarySearch(rand.nextDouble() * cumulative.last())
            return if (idx >= 0) idx else -idx - 1
        }
    }

    private fun skewedHitRate(detectLoops: Boolean): Double {
        val reader = Reader(100, detectLoops)
        val zipf = Zipf(1000, 1)
        repeat(100_000) { reader.read(zipf.next()) }
        return (0 until 100_000).count { reader.read(zipf.next()) } / 100_000.0
    }

    @Test
    fun `a skewed workload hits as often as without loop detection`() {
        val withLoops = skewedHitRate(detectLoops = true)
        val without = skewedHitRate(detectLoops = false)
        assertTrue(withLoops >= without - 0.01, "$withLoops vs $without")
    }

    private fun shiftedHitRate(detectLoops: Boolean): Double {
        val reader = Reader(100, detectLoops)
        val zipf = Zipf(1000, 2)
        val rand = Random(3)
        repeat(100_000) { reader.read(zipf.next()) }

        // the popular set moves on, but the old one is still read now and then
        fun next() = if (rand.nextDouble() < 0.05) zipf.next() else 10_000 + zipf.next()
        repeat(20_000) { reader.read(next()) }
        return (0 until 80_000).count { reader.read(next()) } / 80_000.0
    }

    @Test
    fun `a popular set that moves on is picked up as without loop detection`() {
        val withLoops = shiftedHitRate(detectLoops = true)
        val without = shiftedHitRate(detectLoops = false)
        assertTrue(withLoops >= without - 0.02, "$withLoops vs $without")
    }

    @Test
    fun `a hot set that follows a loop takes the cache over from it`() {
        val reader = Reader(100)
        repeat(20) { reader.hitRate(1000 until 1125) }

        val hot = 0 until 60
        var oneShot = 100_000
        fun pass() = hot.count { key -> reader.read(key).also { reader.read(oneShot++) } }.toDouble() / hot.count()

        repeat(20) { pass() }

        val hitRate = (0 until 10).map { pass() }.average()
        assertTrue(hitRate > 0.95, "hit rate $hitRate")
    }

    @Test
    fun `held entries stay resident through a loop, frozen or not`() {
        val reader = Reader(100)
        val loop = 0 until 125

        repeat(3) { reader.hitRate(loop) }

        val held = reader.resident.values.take(20).onEach { it.isHeld = true }

        repeat(10) { reader.hitRate(loop) }

        held.forEach { assertTrue(reader.resident[it.key] === it, "held entry ${it.key} was evicted") }
    }
}
