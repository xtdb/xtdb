package xtdb.util

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.UnionVector

internal fun BufferAllocator.openChildAllocator(name: String) =
    newChildAllocator(name, 0, Long.MAX_VALUE)

internal fun BufferAllocator.registerMetrics(meterRegistry: MeterRegistry) = apply {
    Gauge.builder("$name.allocator.allocated_memory", this) { al -> al.allocatedMemory.toDouble() }
        .baseUnit("bytes")
        .register(meterRegistry)
}

fun ValueVector.openSlice(offset: Int = 0, len: Int = valueCount): ValueVector =
    when {
        // see #3088
        this is ListVector && len == 0 -> ListVector.empty(name, allocator)

        this is UnionVector && len == 0 ->
            UnionVector.empty(name, allocator).also { it.initializeChildrenFromFields(field.children) }

        else -> getTransferPair(field, allocator).also { it.splitAndTransfer(offset, len) }.to
    }