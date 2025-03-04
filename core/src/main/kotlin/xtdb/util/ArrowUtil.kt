package xtdb.util

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator

internal fun BufferAllocator.openChildAllocator(name: String) =
    newChildAllocator(name, 0, Long.MAX_VALUE)

internal fun BufferAllocator.registerMetrics(meterRegistry: MeterRegistry) = apply {
    Gauge.builder("$name.allocator.allocated_memory", this) { al -> al.allocatedMemory.toDouble() }
        .baseUnit("bytes")
        .register(meterRegistry)
}
