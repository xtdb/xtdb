package xtdb.util

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.UnionVector
import org.apache.arrow.vector.types.pojo.Schema
import java.nio.ByteBuffer
import kotlin.jvm.Throws

internal fun BufferAllocator.openChildAllocator(name: String) =
    newChildAllocator(name, 0, Long.MAX_VALUE)

fun ValueVector.openSlice(offset: Int = 0, len: Int = valueCount): ValueVector =
    when {
        // see #3088
        this is ListVector && len == 0 -> ListVector.empty(name, allocator)

        this is UnionVector && len == 0 ->
            UnionVector.empty(name, allocator).also { it.initializeChildrenFromFields(field.children) }

        else -> getTransferPair(field, allocator).also { it.splitAndTransfer(offset, len) }.to
    }

@Throws(InterruptedException::class)
fun Schema.serializeAsMessageInterruptibly(): ByteArray =
    try {
        serializeAsMessage()
    } catch (e: RuntimeException) {
        throw if (Thread.interrupted()) InterruptedException().initCause(e) else e
    }

@Throws(InterruptedException::class)
fun ByteBuffer.deserializeMessageAsSchemaInterruptibly(): Schema =
    try {
        Schema.deserializeMessage(this)
    } catch (e: RuntimeException) {
        throw if (Thread.interrupted()) InterruptedException().initCause(e) else e
    }
