package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.test.AllocatorResolver
import xtdb.util.Hasher

@ExtendWith(AllocatorResolver::class)
class SingletonListReaderTest {

    private fun openSingleton(al: BufferAllocator) =
        SingletonListReader("col", Vector.fromList(al, "el", I64, listOf(1L, 2L, 3L)))

    @Test
    fun `copying the list copies every element`(al: BufferAllocator) {
        openSingleton(al).use { rdr ->
            al.openVector(rdr.field).use { dest ->
                rdr.rowCopier(dest).copyRow(0)

                assertEquals(listOf(listOf(1L, 2L, 3L)), dest.asList)
            }
        }
    }

    @Test
    fun `the list hashes as the equivalent list vector does`(al: BufferAllocator) {
        openSingleton(al).use { rdr ->
            Vector.fromList(al, "col", listTypeOf(I64), listOf(listOf(1L, 2L, 3L))).use { expected ->
                for (hasher in listOf(Hasher.Xx(), Hasher.Murmur3())) {
                    assertEquals(expected.hashCode(0, hasher), rdr.hashCode(0, hasher))
                }
            }
        }
    }
}
