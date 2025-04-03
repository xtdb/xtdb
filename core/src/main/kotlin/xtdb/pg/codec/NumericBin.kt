/**
 * This file contains code originally released under the Unlicense by [igrishaev/pg2].
 * Original source: https://github.com/igrishaev/pg2/blob/ad5b1f5dbb7b32f32788101bda66b7a7b1e79020/pg-core/src/java/org/pg/codec/NumericBin.java
 *
 * The Unlicense places the original work in the public domain without restrictions.
 * This version is now distributed under  MPL-2.0 license.
 */
@file:JvmName("NumericBin")
package xtdb.pg.codec

import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.nio.ByteBuffer
import java.util.ArrayList

private const val DECIMAL_DIGITS = 4
private val TEN_THOUSAND = BigInteger("10000")

private const val NUMERIC_POS = 0x0000
private const val NUMERIC_NEG = 0x4000

fun encode(value: BigDecimal): ByteBuffer {
    // Number of fractional digits:
    val fractionDigits = value.scale()

    // Number of Fraction Groups:
    val fractionGroups = if (fractionDigits > 0) (fractionDigits + 3) / 4 else 0

    val digits = digits(value)

    val bbLen = 8 + (2 * digits.size)
    val bb = ByteBuffer.allocate(bbLen)

    bb.putShort(digits.size.toShort())
    bb.putShort((digits.size - fractionGroups - 1).toShort())
    bb.putShort((if (value.signum() == 1) NUMERIC_POS else NUMERIC_NEG).toShort())
    bb.putShort(Math.max(fractionDigits, 0).toShort())

    for (pos in digits.indices.reversed()) {
        val valueToWrite = digits[pos]
        bb.putShort(valueToWrite.toShort())
    }

    return bb
}

fun decode(bb: ByteBuffer): BigDecimal {
    val digitsNum = bb.short

    if (digitsNum == 0.toShort()) {
        return BigDecimal.ZERO
    }

    val weight = bb.short
    val sign = bb.short
    val scale = bb.short

    val exp = (weight - digitsNum + 1) * DECIMAL_DIGITS

    var num = BigInteger.ZERO

    for (i in 0 until digitsNum) {
        val base = TEN_THOUSAND.pow((digitsNum - i - 1).toInt())
        val digit = BigInteger.valueOf(bb.short.toLong())
        num = num.add(base.multiply(digit))
    }

    if (sign.toInt() != NUMERIC_POS) {
        num = num.negate()
    }

    return BigDecimal(num)
        .scaleByPowerOfTen(exp)
        .setScale(scale.toInt(), RoundingMode.DOWN)
}

// Inspired by implementation here:
// https://github.com/PgBulkInsert/PgBulkInsert/blob/master/PgBulkInsert/src/main/java/de/bytefish/pgbulkinsert/pgsql/handlers/BigDecimalValueHandler.java
private fun digits(value: BigDecimal): List<Int> {
    var unscaledValue = value.unscaledValue()

    if (value.signum() == -1) {
        unscaledValue = unscaledValue.negate()
    }

    val digits = ArrayList<Int>()

    if (value.scale() > 0) {
        // The scale needs to be a multiple of 4:
        val scaleRemainder = value.scale() % 4

        // Scale the first value:
        if (scaleRemainder != 0) {
            val result = unscaledValue.divideAndRemainder(BigInteger.TEN.pow(scaleRemainder))
            val digit = result[1].toInt() * Math.pow(10.0, (DECIMAL_DIGITS - scaleRemainder).toDouble()).toInt()
            digits.add(digit)
            unscaledValue = result[0]
        }

        while (unscaledValue != BigInteger.ZERO) {
            val result = unscaledValue.divideAndRemainder(TEN_THOUSAND)
            digits.add(result[1].toInt())
            unscaledValue = result[0]
        }
    } else {
        var originalValue = unscaledValue.multiply(BigInteger.TEN.pow(Math.abs(value.scale())))
        while (originalValue != BigInteger.ZERO) {
            val result = originalValue.divideAndRemainder(TEN_THOUSAND)
            digits.add(result[1].toInt())
            originalValue = result[0]
        }
    }

    return digits
}