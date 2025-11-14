@file:JvmName("LeastUpperBound")

package xtdb.types

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.Null
import org.apache.arrow.vector.types.pojo.ArrowType.Duration as DurationType
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint as FloatType
import org.apache.arrow.vector.types.pojo.ArrowType.Int as IntType

private fun leastUpperBound2(left: IntType, right: IntType): ArrowType? =
    if (left.isSigned && right.isSigned) maxOf(left, right, compareBy { it.bitWidth }) else null

private fun leastUpperBound2(left: FloatType, right: FloatType): ArrowType =
    maxOf(left, right, compareBy { it.precision })

private fun leastUpperBound2(left: DurationType, right: DurationType): ArrowType =
    maxOf(left, right, compareBy { it.unit })

private fun leastUpperBound2(left: ArrowType, right: ArrowType): ArrowType? = when {
    left is Null -> right
    right is Null -> left

    left is IntType && right is IntType -> leastUpperBound2(left, right)

    left is IntType && right is FloatType -> right
    left is FloatType && right is IntType -> left
    left is FloatType && right is FloatType -> leastUpperBound2(left, right)

    left is DurationType && right is DurationType -> leastUpperBound2(left, right)

    else -> null
}

fun leastUpperBound(types: Collection<ArrowType>): ArrowType? =
    types.reduceOrNull { l, r -> leastUpperBound2(l, r) ?: return null }
