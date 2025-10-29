package xtdb.util

import java.time.*

enum class TimeUnit {
    SECOND, MINUTE, HOUR, DAY, MONTH, QUARTER, YEAR
}

class MockClock(
    private val instants: Iterator<Instant>
) : InstantSource {

    constructor(instantSequence: Sequence<Instant>) : this(instantSequence.iterator())

    override fun instant(): Instant {
        require(instants.hasNext()) { "MockClock has run out of instants" }
        return instants.next()
    }

    companion object {
        /**
         * Creates a sequence of instants starting from [start], incrementing by [step] [unit]s.
         */
        fun instantSequence(
            unit: TimeUnit,
            step: Long = 1,
            start: Instant = Instant.parse("2020-01-01T00:00:00Z")
        ): Sequence<Instant> = when (unit) {
            TimeUnit.SECOND -> generateSequence(start) { it.plusSeconds(step) }
            TimeUnit.MINUTE -> generateSequence(start) { it.plus(Duration.ofMinutes(step)) }
            TimeUnit.HOUR -> generateSequence(start) { it.plus(Duration.ofHours(step)) }
            TimeUnit.DAY -> generateSequence(start) { it.plus(Duration.ofDays(step)) }
            TimeUnit.MONTH -> {
                val zdt = start.atZone(ZoneOffset.UTC)
                generateSequence(YearMonth.of(zdt.year, zdt.month)) {
                    it.plusMonths(step)
                }.map {
                    it.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant()
                }
            }
            TimeUnit.QUARTER -> {
                val zdt = start.atZone(ZoneOffset.UTC)
                generateSequence(YearMonth.of(zdt.year, zdt.month)) {
                    it.plusMonths(3 * step)
                }.map {
                    it.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant()
                }
            }
            TimeUnit.YEAR -> {
                val zdt = start.atZone(ZoneOffset.UTC)
                generateSequence(YearMonth.of(zdt.year, zdt.month)) {
                    it.plusYears(step)
                }.map {
                    it.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant()
                }
            }
        }

        /**
         * Creates a MockClock with daily instants starting from 2020-01-01.
         */
        fun daily(start: Instant = Instant.parse("2020-01-01T00:00:00Z")): MockClock =
            MockClock(instantSequence(TimeUnit.DAY, start = start))
    }
}
