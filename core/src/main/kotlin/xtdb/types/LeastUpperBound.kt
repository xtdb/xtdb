@file:JvmName("LeastUpperBound")

package xtdb.types

import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.F64_TYPE
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Mono
import xtdb.arrow.VectorType.Scalar
import kotlin.math.max
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal as DecimalType
import org.apache.arrow.vector.types.pojo.ArrowType.Duration as DurationType
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint as FloatType
import org.apache.arrow.vector.types.pojo.ArrowType.Int as IntType
import org.apache.arrow.vector.types.pojo.ArrowType.Time as TimeType
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp as TimestampType

private fun leastUpperBound2(left: IntType, right: IntType): ArrowType? =
    if (left.isSigned && right.isSigned) maxOf(left, right, compareBy { it.bitWidth }) else null

private fun leastUpperBound2(left: FloatType, right: FloatType): ArrowType =
    maxOf(left, right, compareBy { it.precision })

private fun leastUpperBound2(left: DurationType, right: DurationType): ArrowType =
    maxOf(left, right, compareBy { it.unit })

private fun leastUpperBound2(left: TimestampType, right: TimestampType): ArrowType? =
    if (left.timezone == null && right.timezone == null) {
        // timestamp-local: take finer precision
        TimestampType(maxOf(left.unit, right.unit), null)
    } else if (left.timezone != null && right.timezone != null) {
        // timestamp-tz: take finer precision, unify timezone to UTC if different
        val tz = if (left.timezone == right.timezone) left.timezone else "Z"
        TimestampType(maxOf(left.unit, right.unit), tz)
    } else {
        null
    }

private fun leastUpperBound2(left: TimeType, right: TimeType): ArrowType? =
    if (left.bitWidth == right.bitWidth) {
        TimeType(maxOf(left.unit, right.unit), left.bitWidth)
    } else {
        // different bit widths - take the finer precision with appropriate bit width
        val unit = maxOf(left.unit, right.unit)
        val bitWidth = if (unit.ordinal >= 2) 64 else 32 // MICRO/NANO need 64 bits
        TimeType(unit, bitWidth)
    }

private fun leastUpperBound2(left: DecimalType, right: DecimalType): ArrowType {
    // For two decimals, we need to accommodate both the integer and fractional parts
    val leftIntegerDigits = left.precision - left.scale
    val rightIntegerDigits = right.precision - right.scale

    val resultScale = max(left.scale, right.scale)
    val resultIntegerDigits = max(leftIntegerDigits, rightIntegerDigits)
    val resultPrecision = resultIntegerDigits + resultScale

    // Choose bit width to accommodate the precision
    val resultBitWidth = when {
        resultPrecision <= 9 -> 32
        resultPrecision <= 18 -> 64
        resultPrecision <= 38 -> 128
        else -> 256
    }

    return DecimalType(resultPrecision, resultScale, resultBitWidth)
}

private fun leastUpperBound2(left: Mono, right: Mono): Mono? =
    when {
        left == VectorType.Null -> right
        right == VectorType.Null -> left

        left !is Scalar || right !is Scalar -> null

        else -> {
            val leftType = left.arrowType
            val rightType = right.arrowType

            when (leftType) {
                is IntType if rightType is IntType -> leastUpperBound2(leftType, rightType)
                is IntType if rightType is FloatType -> rightType
                is FloatType if rightType is IntType -> leftType
                is FloatType if rightType is FloatType -> leastUpperBound2(leftType, rightType)
                is DecimalType if rightType is IntType -> F64_TYPE
                is IntType if rightType is DecimalType -> F64_TYPE
                is DecimalType if rightType is FloatType -> rightType
                is FloatType if rightType is DecimalType -> leftType
                is DecimalType if rightType is DecimalType -> leastUpperBound2(leftType, rightType)
                is DurationType if rightType is DurationType -> leastUpperBound2(leftType, rightType)
                is TimestampType if rightType is TimestampType -> leastUpperBound2(leftType, rightType)
                is TimeType if rightType is TimeType -> leastUpperBound2(leftType, rightType)
                else -> null
            }?.let { Scalar(it) }
        }
    }

@JvmName("of")
fun leastUpperBound(types: Collection<VectorType>): Mono? =
    types
        .flatMap { it.legs }
        .reduceOrNull { l, r -> leastUpperBound2(l, r) ?: return null }
