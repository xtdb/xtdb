package xtdb.util

import java.time.Instant
import kotlin.math.max
import kotlin.math.min

data class TemporalColumn (
    var lower: Long = Long.MIN_VALUE,
    var upper: Long = Long.MAX_VALUE) {

    fun lt(operand: Long) = apply { upper = min(operand - 1, upper) }
    fun lte(operand: Long) = apply { upper = min(operand, upper) }
    fun gt(operand: Long) = apply { lower = max(operand + 1, lower) }
    fun gte(operand: Long) = apply { lower = max(operand, lower) }

    fun inRange(operand: Long) = operand in lower..upper
    fun intersects(other: TemporalColumn) = lower <= other.upper && other.lower <= upper
    fun union(other: TemporalColumn) = TemporalColumn(
        lower = min(lower, other.lower),
        upper = max(upper, other.upper))

    fun expand(operand: Long) = apply {
        lower = min(lower, operand)
        upper = max(upper, operand)
    }

    override fun toString(): String {
        val l = Instant.ofEpochMilli(lower / 1000)
        val u = Instant.ofEpochMilli(upper / 1000)
        return "TemporalColumn(lower=$l, upper=$u)"
    }
}

data class TemporalBounds(
    val validFrom: TemporalColumn = TemporalColumn(),
    val validTo: TemporalColumn = TemporalColumn(),
    val systemFrom: TemporalColumn = TemporalColumn(),
    val systemTo: TemporalColumn = TemporalColumn()) {

    fun inRange(validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long) =
        (this.validFrom.inRange(validFrom)
            && this.validTo.inRange(validTo)
            && this.systemFrom.inRange(systemFrom)
            && this.systemTo.inRange(systemTo))

    // intersects - There exists at least one interval (rectangle) that is common to both bounds

    fun intersectsValidTime(other: TemporalBounds) = (validFrom.intersects(other.validFrom) && validTo.intersects(other.validTo))
    fun intersectsSystemTime (other: TemporalBounds) = (systemFrom.intersects(other.systemFrom) && systemTo.intersects(other.systemTo))
    fun intersects(other: TemporalBounds) = intersectsValidTime(other) && intersectsSystemTime(other)

    // overlaps - There exists two intervals (rectangles), one from each bound, that overlap

    fun overlapsValidTime(other: TemporalBounds) = (validFrom.intersects(other.validFrom) || validTo.intersects(other.validTo))
    fun overlapsSystemTime(other: TemporalBounds) = (validFrom.intersects(other.validFrom) || validTo.intersects(other.validTo))
    fun overlaps(other: TemporalBounds) = overlapsValidTime(other) && overlapsSystemTime(other)

    // note - this is not a symmetric operation

    fun canBoundValidTime(other: TemporalBounds) =  overlapsValidTime(other) && validTo.lower <= other.validTo.upper
    fun canBoundSytemTime(other: TemporalBounds) =  overlapsSystemTime(other) && systemTo.lower <= other.systemTo.upper
    fun canBound(other: TemporalBounds) = canBoundValidTime(other) && canBoundSytemTime(other)

    fun union(other: TemporalBounds) = TemporalBounds(
        validFrom = validFrom.union(other.validFrom).union(other.validTo),
        validTo = validTo.union(other.validFrom).union(other.validTo),
        systemFrom = systemFrom.union(other.systemFrom),
        systemTo = systemTo.union(other.systemTo))
}