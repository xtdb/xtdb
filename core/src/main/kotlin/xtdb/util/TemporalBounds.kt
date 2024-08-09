package xtdb.util

import java.time.Instant

data class TemporalDimension(
    var lower: Long = Long.MIN_VALUE,
    var upper: Long = Long.MAX_VALUE) {

    fun intersects(other: TemporalDimension) = intersects(other.lower, other.upper)
    fun intersects(lower: Long, upper: Long) = this.lower < upper && lower < this.upper

    override fun toString(): String {
        val l = Instant.ofEpochMilli(lower / 1000)
        val u = Instant.ofEpochMilli(upper / 1000)
        return "TemporalColumn(lower=$l, upper=$u)"
    }

    companion object {
        @JvmStatic
        fun at(at: Long) = TemporalDimension(at, at+1)
        @JvmStatic
        fun `in`(from: Long, to: Long?) = TemporalDimension(from, to ?: Long.MAX_VALUE)
        @JvmStatic
        fun between (from: Long, to: Long?) = TemporalDimension(from, to?.inc() ?: Long.MAX_VALUE)
    }
}

data class TemporalBounds(
    val validTime: TemporalDimension = TemporalDimension(),
    val systemTime: TemporalDimension = TemporalDimension()
){
    fun intersects(other: TemporalBounds) = this.validTime.intersects(other.validTime) && this.systemTime.intersects(other.systemTime)
    fun intersects(validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long) =
        (this.validTime.intersects(validFrom, validTo) && this.systemTime.intersects(systemFrom, systemTo))
}