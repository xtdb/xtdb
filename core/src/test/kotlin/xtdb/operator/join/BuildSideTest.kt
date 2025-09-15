package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.roaringbitmap.RoaringBitmap
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.expression.map.IndexHasher
import xtdb.test.AllocatorResolver
import xtdb.types.Fields
import xtdb.vector.OldRelationWriter

@ExtendWith(AllocatorResolver::class)
class BuildSideTest {

    private fun BuildSide.getMatches(hash: Int): List<Int> {
        val matches = mutableListOf<Int>()
        forEachMatch(hash) { matches.add(it) }
        return matches
    }

    @Test
    fun testBuildSideWithDiskSpill(al: BufferAllocator) {
        val schema = Schema(listOf(
            Fields.I32.nullable.toArrowField("id"),
            Fields.UTF8.nullable.toArrowField("name"),
            Fields.I32.nullable.toArrowField("value")
        ))

        val rows = listOf(
            mapOf("id" to 1, "name" to "John", "value" to 100),
            mapOf("id" to 2, "name" to "Jane", "value" to 200),
            mapOf("id" to 3, "name" to "Bob", "value" to 300)
        )

        val relWriter = OldRelationWriter(al, schema)
        relWriter.writeRows(*rows.toTypedArray())

        relWriter.asReader.use { relation ->
            // without nil row
            BuildSide(al, schema, listOf("id"), RoaringBitmap(), false, 5).use { buildSide ->
                buildSide.append(relation)
                buildSide.append(relation)
                buildSide.append(relation)

                buildSide.build()

                assertEquals(buildSide.toDisk, true)

                val builtRelation = buildSide.builtRel

                assertEquals(9, builtRelation.rowCount)

                assertEquals(rows + rows + rows, builtRelation.toMaps(SNAKE_CASE_STRING))

                val idVector = builtRelation.vectorFor("id")
                val hasher = IndexHasher.fromCols(listOf(idVector))
                val val2Hash = hasher.hashCode(1) // hash for id=2
                val expectedMatches = listOf(1, 4, 7)
                assertEquals(expectedMatches, buildSide.getMatches(val2Hash).sorted())
            }

            // with nil row
            BuildSide(al, schema, listOf("id"), RoaringBitmap(), true, 5).use { buildSide ->
                buildSide.append(relation)
                buildSide.append(relation)
                buildSide.append(relation)

                buildSide.build()

                assertEquals(buildSide.toDisk, true)

                val builtRelation = buildSide.builtRel

                assertEquals(10, builtRelation.rowCount)

                assertEquals(
                    listOf<Map<*, *>>(emptyMap<String, Any>()) + rows + rows + rows,
                    builtRelation.toMaps(SNAKE_CASE_STRING)
                )

                val idVector = builtRelation.vectorFor("id")
                val hasher = IndexHasher.fromCols(listOf(idVector))
                val val2Hash = hasher.hashCode(2) // hash for id=2
                val expectedMatches = listOf(2, 5, 8)
                assertEquals(expectedMatches, buildSide.getMatches(val2Hash).sorted())
            }
        }
    }

    @Test
    fun testBuildSideWithoutDiskSpill(al: BufferAllocator) {
        val schema = Schema(listOf(
            Fields.I32.nullable.toArrowField("id"),
            Fields.UTF8.nullable.toArrowField("name"),
            Fields.I32.nullable.toArrowField("value")
        ))

        val rows = listOf(
            mapOf("id" to 1, "name" to "John", "value" to 100),
            mapOf("id" to 2, "name" to "Jane", "value" to 200),
            mapOf("id" to 3, "name" to "Bob", "value" to 300)
        )

        val relWriter = OldRelationWriter(al, schema)
        relWriter.writeRows(*rows.toTypedArray())

        relWriter.asReader.use { rel ->
            BuildSide(al, schema, listOf("id"), RoaringBitmap(), false, 5).use { buildSide ->
                buildSide.append(rel)

                buildSide.build()

                assertEquals(buildSide.toDisk, false)

                val builtRelation = buildSide.builtRel

                assertEquals(3  , builtRelation.rowCount)

                assertEquals(rows, builtRelation.toMaps(SNAKE_CASE_STRING))
            }
        }
    }
}