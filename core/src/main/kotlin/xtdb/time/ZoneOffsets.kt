package xtdb.time

import xtdb.InternalApi
import java.lang.Math.multiplyExact
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.zone.ZoneRules

/**
 * A zone's UTC offsets, in seconds, looked up by epoch second.
 * A fixed-offset zone answers without a lookup.
 *
 * @suppress
 */
// `@InternalApi` rather than `internal`: the expression engine's generated Clojure calls it over interop,
// which can't resolve a mangled `internal` name - it can only go `internal` if codegen stops emitting these calls.
@InternalApi
sealed interface ZoneOffsets {
    fun offsetAt(epochSecond: Long): Int

    /**
     * The offset at which the wall-clock reading [localSecond] resolves, as `ZonedDateTime.ofLocal(ldt, zone, preferredOffset)`:
     * - an overlap resolves to [preferredOffset] where it is one of the two, else to the earlier offset;
     * - a gap resolves to the offset before it, which moves the reading forward by the gap's length.
     *
     * [NO_PREFERRED_OFFSET] resolves as `ZonedDateTime.of(ldt, zone)` does.
     */
    fun offsetForLocal(localSecond: Long, preferredOffset: Int): Int

    /**
     * The epoch second at which [epochDay] starts, as `LocalDate.atStartOfDay(zone)`:
     * a day whose midnight falls in a gap starts where the gap ends.
     */
    fun startOfDay(epochDay: Long): Long

    @InternalApi
    class Fixed(private val offset: Int) : ZoneOffsets {
        override fun offsetAt(epochSecond: Long) = offset
        override fun offsetForLocal(localSecond: Long, preferredOffset: Int) = offset
        override fun startOfDay(epochDay: Long) = multiplyExact(epochDay, SECS_PER_DAY) - offset
    }

    @InternalApi
    class Region(private val rules: ZoneRules) : ZoneOffsets {
        override fun offsetAt(epochSecond: Long) = rules.getOffset(Instant.ofEpochSecond(epochSecond)).totalSeconds

        override fun offsetForLocal(localSecond: Long, preferredOffset: Int): Int {
            // offsets lie within ±18h, so only a transition inside this window can make a reading ambiguous
            val before = offsetAt(localSecond - MAX_OFFSET_SECS)
            val after = offsetAt(localSecond + MAX_OFFSET_SECS)
            if (before == after) return before

            val beforeValid = offsetAt(localSecond - before) == before
            val afterValid = offsetAt(localSecond - after) == after

            return when {
                afterValid && !beforeValid -> after
                afterValid && preferredOffset == after -> after
                else -> before
            }
        }

        override fun startOfDay(epochDay: Long): Long {
            val midnight = multiplyExact(epochDay, SECS_PER_DAY)
            val offset = offsetForLocal(midnight, NO_PREFERRED_OFFSET)
            return if (offsetAt(midnight - offset) == offset) midnight - offset
            else rules.getTransition(LocalDateTime.ofEpochSecond(midnight, 0, ZoneOffset.UTC)).instant.epochSecond
        }

        private companion object {
            const val MAX_OFFSET_SECS = 18L * 3600
        }
    }

    companion object {
        const val NO_PREFERRED_OFFSET = Int.MIN_VALUE
        private const val SECS_PER_DAY = 86_400L

        @InternalApi
        @JvmStatic
        fun of(zone: ZoneId): ZoneOffsets {
            val rules = zone.rules
            return if (rules.isFixedOffset) Fixed(rules.getOffset(Instant.EPOCH).totalSeconds) else Region(rules)
        }
    }
}
