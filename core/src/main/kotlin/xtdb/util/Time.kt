package xtdb.util

import java.lang.Math.addExact
import java.lang.Math.multiplyExact
import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

internal val Instant.asMicros
    get() = addExact(
        multiplyExact(epochSecond, 1_000_000),
        (nano / 1_000).toLong()
    )

internal fun microsToInstant(micros: Long) = Instant.EPOCH.plus(micros, MICROS)