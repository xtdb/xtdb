package xtdb.cache

import com.sun.nio.file.ExtendedOpenOption
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.CompletableFuture.completedFuture
import kotlin.io.path.pathString
import kotlin.reflect.typeOf

class MemoryCacheTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    inner class PathLoader : MemoryCache.PathLoader {
        private var idx = 0
        override fun load(path: Path): ByteBuffer =
            ByteBuffer.allocateDirect(path.last().pathString.toInt())
                .also { it.put(0, (++idx).toByte()) }

        override fun load(pathSlice: PathSlice): ByteBuffer =
            ByteBuffer.allocateDirect(pathSlice.path.last().pathString.toInt())
                .also { it.put(0, (++idx).toByte()) }
    }

    @Test
    fun `test memory cache`() {
        // just a starter-for-ten here, intent is that we go further down the property/deterministic testing route
        // significantly exercised E2E by the rest of the test-suite and benchmarks.

        MemoryCache(allocator, 250, PathLoader()).use { cache ->

            var t1Evicted = false

            assertAll("get t1", {
                val onEvict = AutoCloseable { t1Evicted = true }

                cache.get(PathSlice(Path.of("t1/100"), 0, 100)) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(1, b1.getByte(0))

                    assertTrue(Stats.compareStats(Stats(100L, 0L, 150L), cache.stats))
                }

                cache.get(PathSlice(Path.of("t1/100"), 0, 100)) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(1, b1.getByte(0))
                }

                Thread.sleep(50)
                assertTrue(Stats.compareStats(Stats(0L, 100L, 150L), cache.stats))
                assertFalse(t1Evicted)
            })

            var t2Evicted = false

            assertAll("t2", {
                val onEvict = AutoCloseable { t2Evicted = true }

                cache.get(PathSlice(Path.of("t2/50"), 0, 50)) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(2, b1.getByte(0))

                    assertTrue(Stats.compareStats(Stats(50L, 100L, 100L), cache.stats))
                }

                Thread.sleep(100)
                assertTrue(Stats.compareStats(Stats(0L, 150L, 100L), cache.stats))
            })

            assertFalse(t1Evicted)
            assertFalse(t2Evicted)

            assertAll("t3 evicts t2/t1", {
                cache.get(PathSlice(Path.of("t3/170"), 0, 170)) { completedFuture(it to null) }.use { b1 ->
                    assertEquals(3, b1.getByte(0))
                    assertTrue(t1Evicted)

                    // definitely needs to evict t1, may or may not evict t2
                    val stats = cache.stats
                    assertEquals(170, stats.pinnedBytes)
                    assertEquals(80, stats.evictableBytes + stats.freeBytes)
                }

                Thread.sleep(100)
                val stats = cache.stats
                assertEquals(0, stats.pinnedBytes)
                assertEquals(250, stats.evictableBytes + stats.freeBytes)
            })
        }
    }

//    fun getDirectBufferAddress(byteBuffer: ByteBuffer): Long {
//        return if (byteBuffer.isDirect) {
//            val method = ByteBuffer::class.java.getDeclaredMethod("address")
//            method.isAccessible = true
//            method.invoke(byteBuffer) as Long
//        } else {
//            throw IllegalArgumentException("ByteBuffer is not direct")
//        }
//    }
//
//    fun getAlignedByteBuffer(size: Int, alignment: Int): ByteBuffer {
//        // Allocate a buffer larger than required to adjust for alignment
//        val buffer = ByteBuffer.allocateDirect(size + alignment - 1)
//
//        println(buffer)
//
//        // Get the memory address of the buffer
//        val address = (buffer as DirectBuffer).address()
//
//        // Calculate the aligned address
//        val offset = (address % alignment).toInt()
//        val padding = if (offset == 0) 0 else alignment - offset
//
//        // Slice the buffer at the aligned position
//        buffer.position(padding)
//        return buffer.slice()
//    }

    private fun getAlignedMemorySegment(arena: Arena, size: Int, alignment: Int): MemorySegment {
        val segment = arena.allocate(size.toLong(), alignment.toLong())
        return segment
    }


    private val BLOCK_SIZE = 4096;

    private fun writeAlignedFile(arena: Arena, path: Path, size: Int) : Int {
        FileChannel.open(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            ExtendedOpenOption.DIRECT
        ).use { channel ->
            val segment = getAlignedMemorySegment(arena, size, BLOCK_SIZE)
            val buffer = segment.asByteBuffer()
            for (i in 0 until size) {
                buffer.put(i.toByte())
            }
            buffer.flip()
            return channel.write(buffer)
        }
    }

    private fun writeNormalFile(path: Path, size: Int) : Int {
        FileChannel.open(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
        ).use { channel ->
            val buffer = ByteBuffer.allocate(size)
            for (i in 0 until size) {
                buffer.put(i.toByte())
            }
            buffer.flip()
            return channel.write(buffer)
        }
    }

    private fun multipleOfBlockSize(size: Long): Int {
        return (((size + BLOCK_SIZE - 1) / BLOCK_SIZE ) * BLOCK_SIZE).toInt()
    }

    private fun multipleOfBlockSizeLower(size: Long): Long {
        val res = multipleOfBlockSize(size)
        return if (res > size) (res - BLOCK_SIZE).toLong() else res.toLong()
    }

    @Test
    fun testFileAlignmentTest() {

        Arena.ofConfined().use { arena ->

            val filePath = Paths.get("example_large_file.txt")
            writeNormalFile(filePath, 10000)
//            writeAlignedFile(arena, filePath, multipleOfBlockSize(10000))


            val start = 398
            val length = 3720

            val ch = FileChannel.open(filePath , setOf(StandardOpenOption.READ, ExtendedOpenOption.DIRECT))
            val size = ch.size()


            val lower = multipleOfBlockSizeLower(start.toLong())
            val lengthUpper = multipleOfBlockSize(length.toLong())

            val segment = getAlignedMemorySegment(arena, lengthUpper, BLOCK_SIZE)
            val bbuf = segment.asByteBuffer()
            ch.read(bbuf, lower)
            bbuf.flip()
            bbuf
        }

    }
}