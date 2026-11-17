package xtdb.bloom

import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN

/**
 * Writes a set of ints in Roaring's portable serialisation, without building a `RoaringBitmap` first.
 *
 * **The format is specified by the [Roaring format spec](https://github.com/RoaringBitmap/RoaringFormatSpec)** — go there rather than trusting this summary.
 * Its "Standard 32-bit Roaring Bitmap" section is the one this writes; the subsections linked from [serialize] are that section's.
 * Where the spec is terse, Roaring 1.6.23's `RoaringArray.serialize` and its containers' `writeArray` are the reference this was checked against.
 *
 * See [serialize] for the format and the algorithm.
 */
object RoaringSerializer {
    // the spec's SERIAL_COOKIE_NO_RUNCONTAINER and SERIAL_COOKIE
    private const val NO_RUN_COOKIE = 12346
    private const val RUN_COOKIE = 12347

    // with run containers, fewer than this many containers omit the offset header
    private const val NO_OFFSET_THRESHOLD = 4
    private const val ARRAY_MAX_CARDINALITY = 4096
    private const val BITMAP_BYTES = 8192

    private enum class Kind { ARRAY, BITMAP, RUN }

    private class Container(val key: Int, val from: Int, val cardinality: Int, val runCount: Int) {
        private val runBytes = 2 + 4 * runCount
        private val plainBytes = if (cardinality <= ARRAY_MAX_CARDINALITY) 2 * cardinality else BITMAP_BYTES

        val kind = when {
            plainBytes > runBytes -> Kind.RUN
            cardinality <= ARRAY_MAX_CARDINALITY -> Kind.ARRAY
            else -> Kind.BITMAP
        }

        val bytes = if (kind == Kind.RUN) runBytes else plainBytes

        fun writeTo(buf: ByteBuffer, sorted: IntArray) {
            val to = from + cardinality

            when (kind) {
                Kind.ARRAY -> for (i in from until to) buf.putShort(sorted[i].toShort())

                Kind.BITMAP -> {
                    val words = LongArray(BITMAP_BYTES / Long.SIZE_BYTES)
                    for (i in from until to) {
                        val low = sorted[i] and 0xFFFF
                        // `shl` on a Long takes the shift mod 64, so this is bit `low & 63` of word `low >>> 6`
                        words[low ushr 6] = words[low ushr 6] or (1L shl low)
                    }
                    words.forEach { buf.putLong(it) }
                }

                Kind.RUN -> {
                    buf.putShort(runCount.toShort())
                    var start = from
                    while (start < to) {
                        var end = start
                        while (end + 1 < to && sorted[end + 1] == sorted[end] + 1) end++
                        buf.putShort(sorted[start].toShort())
                        // a run's length is stored minus one, so a run of 65536 fits in a u16
                        buf.putShort((end - start).toShort())
                        start = end + 1
                    }
                }
            }
        }
    }

    private fun sortedDistinctUnsigned(values: IntArray, n: Int): IntArray {
        // Roaring orders values as unsigned 32-bit; flipping the sign bit makes a signed sort agree with that
        val flipped = IntArray(n) { values[it] xor Int.MIN_VALUE }.also { it.sort() }
        var distinct = 0
        for (v in flipped) if (distinct == 0 || v != flipped[distinct - 1]) flipped[distinct++] = v
        return IntArray(distinct) { flipped[it] xor Int.MIN_VALUE }
    }

    private fun containersOf(sorted: IntArray): List<Container> {
        val containers = mutableListOf<Container>()
        var from = 0
        while (from < sorted.size) {
            val key = sorted[from] ushr 16
            var to = from + 1
            var runCount = 1
            while (to < sorted.size && sorted[to] ushr 16 == key) {
                if (sorted[to] != sorted[to - 1] + 1) runCount++
                to++
            }
            containers += Container(key, from, to - from, runCount)
            from = to
        }
        return containers
    }

    /**
     * Serialises the set of the first [n] of [values] in the [Roaring format](https://github.com/RoaringBitmap/RoaringFormatSpec).
     * Any Roaring implementation reads the bytes back as exactly that set — in Java, `ImmutableRoaringBitmap(buffer)` or `RoaringBitmap.deserialize`.
     *
     * ## The data model
     *
     * Roaring treats each value as an unsigned 32-bit int, and splits it in two:
     * - **the high 16 bits are the key**, which picks a *container*;
     * - **the low 16 bits are stored in that container**, which holds every low part sharing the key.
     *
     * A set is the containers for the keys it uses, in ascending unsigned key order, each holding at least one value.
     *
     * ## Container kinds
     *
     * Each container is stored as one of three kinds, laid out as in the spec's [container storage](https://github.com/RoaringBitmap/RoaringFormatSpec#4-container-storage):
     * - **array** — the low parts as sorted u16s, `2 × cardinality` bytes.
     *   A reader takes a non-run container of cardinality up to 4096 to be an array.
     * - **bitmap** — 65536 bits as 1024 little-endian u64 words, 8192 bytes; value `low` is bit `low & 63` of word `low >>> 6`.
     *   A reader takes a non-run container of cardinality above 4096 to be a bitmap.
     * - **run** — a u16 count of runs, then per run a u16 start and a u16 `length − 1`, `2 + 4 × runs` bytes.
     *   Only the run-flag bitset in the header marks a container as a run.
     *
     * **Every reader decodes every kind, so which kind a container takes is the writer's choice.**
     * This one takes the smallest: a run where it is strictly smaller than the array or bitmap the cardinality would otherwise give, and otherwise that array or bitmap.
     * That is the size rule Roaring's own `runOptimize` applies to an array or bitmap container.
     *
     * ## Layout
     *
     * Everything is little-endian, and the stream is the header, then each container's body in key order.
     * The header is three parts:
     * 1. **The [cookie header](https://github.com/RoaringBitmap/RoaringFormatSpec#1-cookie-header)**, which depends on whether any container is a run.
     *    - With none, a u32 `12346` (`SERIAL_COOKIE_NO_RUNCONTAINER`), then the container count as a u32.
     *    - With any, a u32 whose low 16 bits are `12347` (`SERIAL_COOKIE`) and whose high 16 bits are the container count minus one.
     *      It is followed by the run flags: a bitset of `⌈count / 8⌉` bytes, bit `i % 8` of byte `i / 8` set where container `i` is a run.
     * 2. **The [descriptive header](https://github.com/RoaringBitmap/RoaringFormatSpec#2-descriptive-header)**: per container, its u16 key then its cardinality minus one as a u16, so that a full container's 65536 fits.
     * 3. **The [offset header](https://github.com/RoaringBitmap/RoaringFormatSpec#3-offset-header)**: per container, the u32 byte offset of its body from the start of the stream, cookie included.
     *    It is omitted where any container is a run and there are fewer than four containers; it serves random access, and a sequential reader skips it.
     *
     * ## The algorithm
     *
     * 1. **Sort the values unsigned and drop duplicates.**
     *    Flipping the sign bit maps unsigned order onto signed order, so a plain `IntArray.sort` does it.
     * 2. **Group the sorted values by key**, counting each group's cardinality and its runs of consecutive values in the same pass.
     * 3. **Choose each container's kind by size**, from its cardinality and run count alone.
     * 4. **Size the whole output** from the header's shape and each container's size, and allocate it once.
     * 5. **Write the header, then each container's body** — the offsets are running sums of the bodies' sizes.
     *
     * `RoaringSerializerTest` pins that Roaring reads back exactly the distinct values written and consumes exactly the bytes written, across every container kind, both sides of the offset-header threshold and negative ints; byte identity with Roaring's own `serialize` is not pinned.
     *
     * @param values the first [n] are the set's members, in any order and with duplicates allowed
     * @return a little-endian buffer holding exactly the serialised bytes, positioned at its start
     */
    fun serialize(values: IntArray, n: Int): ByteBuffer {
        val sorted = sortedDistinctUnsigned(values, n)
        val containers = containersOf(sorted)
        val size = containers.size
        val hasRuns = containers.any { it.kind == Kind.RUN }
        val hasOffsets = !hasRuns || size >= NO_OFFSET_THRESHOLD
        // one run-flag bit per container, present only after the run cookie
        val runFlagBytes = if (hasRuns) (size + 7) / 8 else 0

        val headerBytes =
            (if (hasRuns) 4 + runFlagBytes else 8) + 4 * size + (if (hasOffsets) 4 * size else 0)

        val buf = ByteBuffer.allocate(headerBytes + containers.sumOf { it.bytes }).order(LITTLE_ENDIAN)

        if (hasRuns) {
            // the run cookie carries the container count, minus one, in its high 16 bits
            buf.putInt(RUN_COOKIE or ((size - 1) shl 16))
            val runFlags = ByteArray(runFlagBytes)
            containers.forEachIndexed { i, c ->
                if (c.kind == Kind.RUN) runFlags[i / 8] = (runFlags[i / 8].toInt() or (1 shl (i % 8))).toByte()
            }
            buf.put(runFlags)
        } else {
            buf.putInt(NO_RUN_COOKIE)
            buf.putInt(size)
        }

        for (c in containers) {
            buf.putShort(c.key.toShort())
            // stored minus one, so a full container's 65536 fits in a u16
            buf.putShort((c.cardinality - 1).toShort())
        }

        if (hasOffsets) {
            // offsets count from the start of the stream, cookie included
            var offset = headerBytes
            for (c in containers) {
                buf.putInt(offset)
                offset += c.bytes
            }
        }

        for (c in containers) c.writeTo(buf, sorted)

        check(!buf.hasRemaining())
        return buf.flip()
    }
}
