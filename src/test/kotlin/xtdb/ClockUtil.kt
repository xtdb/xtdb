package xtdb

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

class ClockUtil {
    companion object {
        fun createClock(step: Duration, initialTime: Instant = Instant.now()): Clock {
            return object : Clock() {
                private var currentTime = initialTime

                override fun instant(): Instant {
                    val result = currentTime
                    currentTime = currentTime.plus(step)
                    return result
                }

                override fun getZone(): ZoneId = ZoneId.systemDefault()

                override fun withZone(zone: ZoneId?): Clock = this
            }
        }
    }
}