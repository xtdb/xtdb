@file:JvmName("LeastUpperBound")

package xtdb.types

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.Null
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal as DecimalType
import org.apache.arrow.vector.types.pojo.ArrowType.Duration as DurationType
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint as FloatType
import org.apache.arrow.vector.types.pojo.ArrowType.Int as IntType
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp as TimestampType
import org.apache.arrow.vector.types.pojo.ArrowType.Time as TimeType
import kotlin.math.max

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

private fun leastUpperBound2(left: ArrowType, right: ArrowType): ArrowType? = when {
    left is Null -> right
    right is Null -> left

    left is IntType && right is IntType -> leastUpperBound2(left, right)

    left is IntType && right is FloatType -> right
    left is FloatType && right is IntType -> left
    left is FloatType && right is FloatType -> leastUpperBound2(left, right)

    left is DecimalType && right is IntType -> F64.arrowType
    left is IntType && right is DecimalType -> F64.arrowType
    left is DecimalType && right is FloatType -> right
    left is FloatType && right is DecimalType -> left
    left is DecimalType && right is DecimalType -> leastUpperBound2(left, right)

    left is DurationType && right is DurationType -> leastUpperBound2(left, right)

    left is TimestampType && right is TimestampType -> leastUpperBound2(left, right)
    left is TimeType && right is TimeType -> leastUpperBound2(left, right)

    else -> null
}

@JvmName("of")
fun leastUpperBound(types: Collection<VectorType>): ArrowType? =
    types
        .flatMap { it.legs }
        .map { it.arrowType }
        .reduceOrNull { l, r -> leastUpperBound2(l, r) ?: return null }
