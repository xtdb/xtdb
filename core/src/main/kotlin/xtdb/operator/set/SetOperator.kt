package xtdb.operator.set

import xtdb.api.ICursor
import xtdb.api.error.Incorrect
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.VectorTypes

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
}
