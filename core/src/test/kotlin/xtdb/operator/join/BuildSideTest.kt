package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import xtdb.expression.map.IndexHasher.Companion.hasher
import xtdb.test.AllocatorResolver
import xtdb.types.Type
import xtdb.types.Type.Companion.I32
import xtdb.types.Type.Companion.maybe
import xtdb.types.Type.Companion.ofType
import xtdb.types.schema
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
        val schema = schema(
            "id" ofType maybe(I32),
            "name" ofType maybe(Type.UTF8),
            "value" ofType maybe(I32)
        )

        val john = mapOf("id" to 1, "name" to "John", "value" to 100)
        val jane = mapOf("id" to 2, "name" to "Jane", "value" to 200)
        val bob = mapOf("id" to 3, "name" to "Bob", "value" to 300)

        val rows = listOf(john, jane, bob)

        val partOneRows = listOf(bob, bob, bob)
        val partTwoRows = listOf(john, jane, john, jane, john, jane)

        Relation.openFromRows(al, rows).use { rel ->
            // without nil row
            BuildSide(
                al, schema, listOf("id"),
                trackUnmatchedIdxs = false,
                withNilRow = false,
                inMemoryThreshold = 5
            ).use { buildSide ->
                buildSide.append(rel)
                buildSide.append(rel)
                buildSide.append(rel)

                buildSide.build()

                assertNotNull(buildSide.spill)

                val builtRelation = buildSide.builtRel

                assertEquals(9, builtRelation.rowCount)

                assertEquals(
                    partOneRows + partTwoRows,
                    builtRelation.toMaps(SNAKE_CASE_STRING)
                )

                val hasher = rel.hasher(listOf("id"))
                val val2Hash = hasher.hashCode(1) // hash for id=2
                val expectedMatches = listOf(4, 6, 8)
                assertEquals(expectedMatches, buildSide.getMatches(val2Hash).sorted())
            }

            // with nil row
            BuildSide(
                al, schema, listOf("id"),
                trackUnmatchedIdxs = false,
                withNilRow = true,
                inMemoryThreshold = 5
            ).use { buildSide ->
                buildSide.append(rel)
                buildSide.append(rel)
                buildSide.append(rel)

                buildSide.build()

                assertNotNull(buildSide.spill)

                val builtRelation = buildSide.builtRel

                assertEquals(10, builtRelation.rowCount)

                assertEquals(
                    partOneRows + partTwoRows + emptyMap(),
                    builtRelation.toMaps(SNAKE_CASE_STRING)
                )

                val hasher = rel.hasher(listOf("id"))
                val val2Hash = hasher.hashCode(2) // hash for id=3
                val expectedMatches = listOf(0, 1, 2)
                assertEquals(expectedMatches, buildSide.getMatches(val2Hash).sorted())
            }
        }
    }

    @Test
    fun testBuildSideWithoutDiskSpill(al: BufferAllocator) {
        val schema = schema(
            "id" ofType maybe(I32),
            "name" ofType maybe(Type.UTF8),
            "value" ofType maybe(I32)
        )

        val rows = listOf(
            mapOf("id" to 1, "name" to "John", "value" to 100),
            mapOf("id" to 2, "name" to "Jane", "value" to 200),
            mapOf("id" to 3, "name" to "Bob", "value" to 300)
        )

        val relWriter = OldRelationWriter(al, schema)
        relWriter.writeRows(*rows.toTypedArray())

        relWriter.asReader.use { rel ->
            BuildSide(
                al, schema, listOf("id"),
                trackUnmatchedIdxs = false,
                withNilRow = false,
                inMemoryThreshold = 5
            ).use { buildSide ->
                buildSide.append(rel)

                buildSide.build()

                assertNull(buildSide.spill)

                val builtRelation = buildSide.builtRel

                assertEquals(3, builtRelation.rowCount)

                assertEquals(rows, builtRelation.toMaps(SNAKE_CASE_STRING))
            }
        }
    }
}