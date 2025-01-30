@file:JvmName("Time")

package xtdb.time

import java.lang.Math.multiplyExact

internal const val MILLI_HZ = 1_000
internal const val MICRO_HZ = 1_000_000
internal const val NANO_HZ = 1_000_000_000

internal val Long.secondsToMillis get() = multiplyExact(this, MILLI_HZ)
internal val Long.secondsToMicros get() = multiplyExact(this, MICRO_HZ)
internal val Long.secondsToNanos get() = multiplyExact(this, NANO_HZ)

internal val Long.millisToSecondsPart get() = this / MILLI_HZ
internal val Long.microsToSecondsPart get() = this / MICRO_HZ
internal val Long.nanosToSecondsPart get() = this / NANO_HZ

internal val Int.nanoPartToMillis get() = this / (NANO_HZ / MILLI_HZ)
internal val Int.nanoPartToMicros get() = this / (NANO_HZ / MICRO_HZ)

internal val Long.millisToNanosPart get() = multiplyExact(this % MILLI_HZ, (NANO_HZ / MILLI_HZ))
internal val Long.microsToNanosPart get() = multiplyExact(this % MICRO_HZ, (NANO_HZ / MICRO_HZ))
internal val Long.nanosToNanosPart get() = this % NANO_HZ

