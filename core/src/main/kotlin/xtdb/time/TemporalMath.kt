package xtdb.time

import xtdb.InternalApi
import java.lang.Math.addExact
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.lang.Math.multiplyExact
import java.time.LocalDate
import java.time.temporal.IsoFields

/**
 * Zone and unit arithmetic over the expression engine's epoch-unit `long`s.
 *
 * - `hz` is a timestamp's units per second: 1, 1_000, 1_000_000 or 1_000_000_000.
 * - A local timestamp is its wall-clock reading counted from the epoch as though it were UTC - the representation `timestamp-local` already has.
 * - Days are epoch days, as `LocalDate.toEpochDay`.
 *
 * @suppress
 */
// `@InternalApi` rather than `internal`: the expression engine's generated Clojure calls it over interop,
// which can't resolve a mangled `internal` name - it can only go `internal` if codegen stops emitting these calls.
@InternalApi
object TemporalMath {
    private const val SECS_PER_DAY = 86_400L

    @JvmStatic
    fun toLocal(ts: Long, hz: Long, zone: ZoneOffsets): Long =
        addExact(ts, multiplyExact(zone.offsetAt(floorDiv(ts, hz)).toLong(), hz))

    @JvmStatic
    fun epochDay(localTs: Long, hz: Long): Long = floorDiv(localTs, hz * SECS_PER_DAY)

    @JvmStatic
    fun secondOfDay(localTs: Long, hz: Long): Int = (floorMod(localTs, hz * SECS_PER_DAY) / hz).toInt()

    @JvmStatic
    fun year(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).year

    @JvmStatic
    fun month(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).monthValue

    @JvmStatic
    fun dayOfMonth(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).dayOfMonth

    @JvmStatic
    fun dayOfYear(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).dayOfYear

    @JvmStatic
    fun quarter(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).get(IsoFields.QUARTER_OF_YEAR)

    /** Monday = 1 .. Sunday = 7 */
    @JvmStatic
    fun isoDayOfWeek(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).dayOfWeek.value

    /** Sunday = 0 .. Saturday = 6 */
    @JvmStatic
    fun dayOfWeek(epochDay: Long): Int = isoDayOfWeek(epochDay) % 7

    @JvmStatic
    fun isoWeek(epochDay: Long): Int = LocalDate.ofEpochDay(epochDay).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
}
