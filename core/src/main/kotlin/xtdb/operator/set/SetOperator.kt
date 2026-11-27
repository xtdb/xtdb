package xtdb.operator.set

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.api.error.Incorrect
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.VectorTypes
import xtdb.operator.join.BuildSide
import xtdb.operator.join.ComparatorFactory
import xtdb.operator.join.ProbeSide
import xtdb.operator.set.IntersectionCursor.Mode
import xtdb.util.closeOnCatch
import java.time.ZoneId

/**
 * A set operator as planned: its output types, merged from both sides, and how to open its cursor per query.
 *
 * @throws Incorrect `union-incompatible-cols` if the two sides' column names differ.
 */
sealed class SetOperator(leftTypes: VectorTypes, rightTypes: VectorTypes) {

    val vecTypes: VectorTypes = run {
        if (leftTypes.keys != rightTypes.keys)
            throw Incorrect(
                "union incompatible cols", "union-incompatible-cols",
                mapOf("left-col-names" to leftTypes.keys, "right-col-names" to rightTypes.keys)
            )

        leftTypes.mapValues { (colName, leftType) -> mergeTypes(leftType, rightTypes.getValue(colName)) }
    }

    class UnionAll(leftTypes: VectorTypes, rightTypes: VectorTypes) : SetOperator(leftTypes, rightTypes) {
        fun openCursor(left: ICursor, right: ICursor): ICursor = UnionAllCursor(left, right)
    }

    sealed class Matching(
        private val mode: Mode, private val leftTypes: VectorTypes, rightTypes: VectorTypes,
    ) : SetOperator(leftTypes, rightTypes) {

        fun openCursor(
            al: BufferAllocator, zone: ZoneId, inMemoryThreshold: Long,
            left: ICursor, right: ICursor,
        ): ICursor {
            val keyColNames = vecTypes.keys.toList()
            val cmpFactory = ComparatorFactory(nullsEqual = true, zone, theta = null)

            return BuildSide(al, leftTypes, keyColNames, false, false, inMemoryThreshold).closeOnCatch { buildSide ->
                ProbeSide(al, buildSide, keyColNames, cmpFactory).closeOnCatch { probeSide ->
                    IntersectionCursor(left, right, buildSide, probeSide, mode)
                }
            }
        }
    }

    class Intersect(leftTypes: VectorTypes, rightTypes: VectorTypes) : Matching(Mode.INTERSECT, leftTypes, rightTypes)
    class Difference(leftTypes: VectorTypes, rightTypes: VectorTypes) : Matching(Mode.DIFFERENCE, leftTypes, rightTypes)
}
