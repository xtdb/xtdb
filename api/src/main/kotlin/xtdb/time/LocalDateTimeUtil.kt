package xtdb.time

import java.lang.Math.addExact
import java.time.LocalDateTime
import java.time.ZoneOffset.UTC

object LocalDateTimeUtil {
    private val LocalDateTime.epochSecond get() = toEpochSecond(UTC)

    @JvmStatic
    val LocalDateTime.asSeconds get() = epochSecond

    @JvmStatic
    val LocalDateTime.asMillis get() = addExact(epochSecond.secondsToMillis, nano.nanoPartToMillis.toLong())

    @JvmStatic
    val LocalDateTime.asMicros get() = addExact(epochSecond.secondsToMicros, nano.nanoPartToMicros.toLong())

    @JvmStatic
    val LocalDateTime.asNanos get() = addExact(epochSecond.secondsToNanos, nano.toLong())

    private fun ldt(seconds: Long, nanos: Long) = LocalDateTime.ofEpochSecond(seconds, nanos.toInt(), UTC)

    @JvmStatic
    fun fromSeconds(seconds: Long) = ldt(seconds, 0)

    @JvmStatic
    fun fromMillis(millis: Long) = ldt(millis.millisToSecondsPart, millis.millisToNanosPart)

    @JvmStatic
    fun fromMicros(micros: Long) = ldt(micros.microsToSecondsPart, micros.microsToNanosPart)

    @JvmStatic
    fun fromNanos(nanos: Long) = ldt(nanos.nanosToSecondsPart, nanos.nanosToNanosPart)
}
