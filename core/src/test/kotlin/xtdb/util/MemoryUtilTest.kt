package xtdb.util

import org.apache.arrow.memory.AllocationListener
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class MemoryUtilTest {

    @Test
    fun `netty size classes`() {
        assertEquals(16L, nettySizeClass(1))
        assertEquals(48L, nettySizeClass(33))
        assertEquals(80L, nettySizeClass(65))
        assertEquals(128L, nettySizeClass(128))
        assertEquals(160L, nettySizeClass(129))
        assertEquals(640L * 1024, nettySizeClass(600L * 1024))
        assertEquals(1L shl 20, nettySizeClass(1L shl 20))
        assertEquals((64L shl 20) + 1, nettySizeClass((64L shl 20) + 1))
    }

    @Test
    fun `the root allocator accounts a buffer at its netty size class, not the next power of two`() {
        openRootAllocator().use { al ->
            al.buffer(600L * 1024).use { buf ->
                assertEquals(640L * 1024, buf.capacity())
                assertEquals(640L * 1024, al.allocatedMemory)
            }
        }
    }

    @Test
    fun `the root allocator's own listener still hears about child allocators`() {
        val children = mutableListOf<String>()
        val listener = object : AllocationListener {
            override fun onChildAdded(parentAllocator: BufferAllocator, childAllocator: BufferAllocator) {
                children += childAllocator.name
            }
        }

        openRootAllocator(listener = listener).use { al ->
            al.newChildAllocator("child", 0, Long.MAX_VALUE).close()
        }

        assertEquals(listOf("child"), children)
    }
}
