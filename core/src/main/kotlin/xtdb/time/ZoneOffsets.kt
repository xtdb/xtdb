package xtdb.time

import xtdb.InternalApi
import java.time.Instant
import java.time.ZoneId
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

    @InternalApi
    class Fixed(private val offset: Int) : ZoneOffsets {
        override fun offsetAt(epochSecond: Long) = offset
    }

    @InternalApi
    class Region(private val rules: ZoneRules) : ZoneOffsets {
        override fun offsetAt(epochSecond: Long) = rules.getOffset(Instant.ofEpochSecond(epochSecond)).totalSeconds
    }

    companion object {
        @InternalApi
        @JvmStatic
        fun of(zone: ZoneId): ZoneOffsets {
            val rules = zone.rules
            return if (rules.isFixedOffset) Fixed(rules.getOffset(Instant.EPOCH).totalSeconds) else Region(rules)
        }
    }
}
