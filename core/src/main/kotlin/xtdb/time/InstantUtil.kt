package xtdb.time

import java.lang.Math.addExact
import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

object InstantUtil {

    @JvmStatic
    val Instant.asMicros get() = addExact(epochSecond.secondsToMicros, nano.nanoPartToMicros.toLong())

    @JvmStatic
    fun fromMicros(micros: Long) = Instant.EPOCH.plus(micros, MICROS)
}