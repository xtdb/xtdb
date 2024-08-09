package xtdb.util

import java.time.Instant
import kotlin.math.max
import kotlin.math.min

data class TemporalDimension(
    var lower: Long = Long.MIN_VALUE,
    var upper: Long = Long.MAX_VALUE) {

    fun lt(operand: Long) = apply { upper = min(operand - 1, upper) }
    fun lte(operand: Long) = apply { upper = min(operand, upper) }
    fun gt(operand: Long) = apply { lower = max(operand + 1, lower) }
    fun gte(operand: Long) = apply { lower = max(operand, lower) }

    fun intersects(other: TemporalDimension) = lower <= other.upper && other.lower <= upper
    fun intersects(lower: Long, upper: Long) = this.lower <= upper && lower <= this.upper

    override fun toString(): String {
        val l = Instant.ofEpochMilli(lower / 1000)
        val u = Instant.ofEpochMilli(upper / 1000)
        return "TemporalColumn(lower=$l, upper=$u)"
    }
}

data class TemporalBounds(
    val validTime: TemporalDimension = TemporalDimension(),
    val systemTime: TemporalDimension = TemporalDimension()
){
    fun intersects(other: TemporalBounds) = this.validTime.intersects(other.validTime) && this.systemTime.intersects(other.systemTime)
    fun intersects(validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long) =
        (this.validTime.intersects(validFrom, validTo-1) && this.systemTime.intersects(systemFrom, systemTo-1))
}