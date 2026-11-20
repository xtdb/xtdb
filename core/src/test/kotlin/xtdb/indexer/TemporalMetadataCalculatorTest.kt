package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.TimestampTzVector
import xtdb.log.proto.TemporalMetadata
import xtdb.test.AllocatorResolver
import java.time.ZoneOffset

@ExtendWith(AllocatorResolver::class)
class TemporalMetadataCalculatorTest {

    private fun BufferAllocator.instants(name: String, vararg micros: Long) =
        TimestampTzVector(this, name, false, TimeUnit.MICROSECOND, ZoneOffset.UTC)
            .apply { micros.forEach { writeLong(it) } }

    private fun seeded() = TemporalMetadata.newBuilder()
        .setMinValidFrom(Long.MAX_VALUE).setMaxValidFrom(Long.MIN_VALUE)
        .setMinValidTo(Long.MAX_VALUE).setMaxValidTo(Long.MIN_VALUE)
        .setMinSystemFrom(Long.MAX_VALUE).setMaxSystemFrom(Long.MIN_VALUE)

    @Test
    fun `update folds each column's min and max into the builder`(al: BufferAllocator) {
        al.instants("_valid_from", 30, -10, 20).use { vf ->
            al.instants("_valid_to", 100, Long.MAX_VALUE, 50).use { vt ->
                al.instants("_system_from", 7, 3, 5).use { sf ->
                    val tm = seeded()
                        .setMinValidFrom(0).setMaxSystemFrom(9)
                        .update(vf, vt, sf)
                        .build()

                    assertEquals(-10, tm.minValidFrom)
                    assertEquals(30, tm.maxValidFrom)
                    assertEquals(50, tm.minValidTo)
                    assertEquals(Long.MAX_VALUE, tm.maxValidTo)
                    assertEquals(3, tm.minSystemFrom)
                    assertEquals(9, tm.maxSystemFrom, "a builder's existing bound stands where the page's is narrower")
                }
            }
        }
    }

    @Test
    fun `update over empty vectors leaves the builder unchanged`(al: BufferAllocator) {
        al.instants("_valid_from").use { vf ->
            al.instants("_valid_to").use { vt ->
                al.instants("_system_from").use { sf ->
                    val before = seeded().setMinValidFrom(4).setMaxValidTo(8).build()

                    assertEquals(before, before.toBuilder().update(vf, vt, sf).build())
                }
            }
        }
    }
}
