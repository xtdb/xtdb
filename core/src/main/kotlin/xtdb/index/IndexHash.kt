package xtdb.index

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import xtdb.arrow.DateDayVector
import xtdb.arrow.TimestampLocalVector
import xtdb.arrow.TimestampTzVector
import xtdb.arrow.Vector
import xtdb.arrow.VectorReader
import xtdb.time.NANO_HZ
import xtdb.time.hz
import xtdb.util.Hasher
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset.UTC

/**
 * A value's 64-bit index hash: two independently seeded [Hasher.Xx] hashes over the normalisation [VectorReader.hashCode] applies,
 * so values that `=` holds equal hash equal across a union's legs.
 *
 * `-0.0` hashes as `0.0`, which `=` holds equal to it and the 32-bit hash does not.
 *
 * Dates and timestamps are the exception, so [ofLocal] and [ofInstant] give every hash a column value `=` holds equal to one of them can have:
 * - `=` compares them across their types in the session's zone, while a date or local timestamp hashes its wall-clock reading, and a timestamp with a time zone its instant;
 * - each hashes its seconds since the epoch as a double, which a count of nanoseconds doesn't always reach exactly, so one value's hash can differ between precisions.
 */
object IndexHash {
    private class ZeroFolding(seed: Int) : Hasher() {
        private val xx = Xx(seed)

        override fun hash(value: Double) = xx.hash(if (value == 0.0) 0.0 else value)
        override fun hash(value: Long) = xx.hash(value)
        override fun hash(value: ByteArray) = xx.hash(value)
        override fun hash(value: ByteBuffer) = xx.hash(value)
        override fun hash(buf: ArrowBuf, offset: Int, length: Int) = xx.hash(buf, offset, length)
    }

    private val high = ZeroFolding(0)
    private val low = ZeroFolding(0x5bd1e995)

    @JvmStatic
    fun of(rdr: VectorReader, idx: Int): Long =
        (rdr.hashCode(idx, high).toLong() shl 32) or (rdr.hashCode(idx, low).toLong() and 0xffffffffL)

    private fun Vector.hashOf(value: Any) = use { it.writeObject(value); of(it, 0) }

    private fun unitsHolding(nanoOfSecond: Int) = TimeUnit.entries.filter { nanoOfSecond % (NANO_HZ / it.hz) == 0 }

    private fun localHashes(al: BufferAllocator, local: LocalDateTime) =
        unitsHolding(local.nano).map { TimestampLocalVector(al, "v", false, it).hashOf(local) } +
            listOfNotNull(local.takeIf { it.toLocalTime() == LocalTime.MIDNIGHT }?.let { DateDayVector(al, "v", false).hashOf(it.toLocalDate()) })

    private fun instantHashes(al: BufferAllocator, instant: Instant) =
        unitsHolding(instant.nano).map { TimestampTzVector(al, "v", false, it, UTC).hashOf(instant) }

    /**
     * The hashes of every date and timestamp that `=` holds equal to the wall-clock reading [local] in [zone]:
     * - [local] as a local timestamp, and as a date where it is midnight;
     * - each instant [zone] reads as [local] - none in a gap, two in an overlap;
     * - each at every precision that holds it exactly.
     *
     * A date operand is its midnight.
     */
    @JvmStatic
    fun ofLocal(al: BufferAllocator, local: LocalDateTime, zone: ZoneId): Set<Long> =
        (localHashes(al, local) + zone.rules.getValidOffsets(local).flatMap { instantHashes(al, local.toInstant(it)) }).toSet()

    /**
     * The hashes of every date and timestamp that `=` holds equal to [instant] in [zone]:
     * - [instant] as a timestamp with a time zone, whichever zone;
     * - its wall-clock reading in [zone], as a local timestamp, and as a date where it is midnight;
     * - each at every precision that holds it exactly.
     */
    @JvmStatic
    fun ofInstant(al: BufferAllocator, instant: Instant, zone: ZoneId): Set<Long> =
        (instantHashes(al, instant) + localHashes(al, LocalDateTime.ofInstant(instant, zone))).toSet()
}
