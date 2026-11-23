package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.TaggedValue
import xtdb.test.AllocatorResolver
import java.math.BigDecimal

@ExtendWith(AllocatorResolver::class)
class UnionLegCopyTest {

    private fun weirdDecimal(al: BufferAllocator, name: String) =
        DecimalVector(al, name, false, ArrowType.Decimal(38, 2, 128))

    private fun oddLegDuv(al: BufferAllocator): DenseUnionVector {
        val duv = DenseUnionVector(al, "c", listOf(weirdDecimal(al, "weird")))
        duv.vectorFor("weird").writeObject(BigDecimal("1.25"))
        return duv
    }

    @Test
    fun `struct copy of a union child with an odd-named decimal leg`(al: BufferAllocator) {
        StructVector(al, "s", false, linkedMapOf("c" to oddLegDuv(al))).use { src ->
            src.endStruct()
            StructVector(al, "d", false).use { dest ->
                dest.writeValue(src.valueReader().also { it.pos = 0 })
                val row = dest.asList.single() as Map<*, *>
                assertEquals(listOf(BigDecimal("1.25")), row.values.toList())
            }
        }
    }

    @Test
    fun `list copy of union elements with an odd-named decimal leg`(al: BufferAllocator) {
        ListVector(al, "l", false, oddLegDuv(al)).use { src ->
            src.endList()
            ListVector(al, "d", false, DenseUnionVector(al, "\$data\$", listOf(weirdDecimal(al, "weird")))).use { dest ->
                dest.writeValue(src.valueReader().also { it.pos = 0 })
                assertEquals(listOf(listOf(TaggedValue(Keyword.intern("weird"), BigDecimal("1.25")))), dest.asList)
            }
        }
    }

    @Test
    fun `toList tags an odd-named leg as getObject does`(al: BufferAllocator) {
        oddLegDuv(al).use { duv ->
            val tagged = TaggedValue(Keyword.intern("weird"), BigDecimal("1.25"))
            assertEquals(listOf<Any?>(tagged), duv.toList { it })
            assertEquals(listOf<Any?>(tagged), duv.asList)
        }
    }
}
