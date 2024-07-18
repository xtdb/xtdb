package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption.*
import kotlin.io.path.Path

class RelationTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun testUnload() {
        IntVector(Field.nonNullableI32("a"), allocator).use { a ->
            val rel = Relation(listOf(a))

            rel.startUnload(FileChannel.open(Path("/tmp/test.arrow"), CREATE, WRITE, TRUNCATE_EXISTING))
                .use { unloader ->
                    a.writeInt(1)
                    rel.endRow()
                    a.writeInt(2)
                    rel.endRow()

                    unloader.writeBatch()
                    rel.reset()

                    a.writeInt(3)
                    rel.endRow()

                    unloader.writeBatch()

                    unloader.endFile()
                }

        }
    }

}