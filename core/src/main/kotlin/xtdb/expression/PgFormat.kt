@file:JvmName("PgFormat")

package xtdb.expression

import kotlin.math.abs

private val FORMAT_SPECIFIER_PATTERN =
    Regex("""%(?:(\d+)\$)?(-)?(?:(\d+)|\*(?:(\d+)\$)?)?([sIL%])""")

private fun quoteIdent(s: String?): String {
    if (s == null) {
        throw IllegalArgumentException("null values cannot be formatted as an SQL identifier")
    }

    val needsQuoting = s.isEmpty() ||
            !s[0].isLetter() ||
            s.any { !it.isLetterOrDigit() && it != '_' } ||
            s != s.lowercase()

    return if (needsQuoting) "\"${s.replace("\"", "\"\"")}\"" else s
}

private fun quoteLiteral(s: String?): String =
    if (s == null) "NULL" else "'${s.replace("'", "''")}'"

/**
 * PostgreSQL-compatible format function.
 *
 * Format specifiers: %[position][flags][width]type
 * - position: n$ where n is 1-based argument index
 * - flags: - for left-justify
 * - width: minimum characters (number, *, or *n$)
 * - type: s (string), I (identifier), L (literal), % (literal %)
 *
 * @param formatStr the format string
 * @param args arguments, already resolved to strings (or null)
 */
fun pgFormat(formatStr: String, args: List<String?>): String {
    val argCount = args.size
    val sb = StringBuilder()
    var lastConsumedIdx = -1
    var lastEnd = 0

    for (match in FORMAT_SPECIFIER_PATTERN.findAll(formatStr)) {
        val start = match.range.first
        val end = match.range.last + 1
        sb.append(formatStr, lastEnd, start)

        val positionStr = match.groupValues[1].ifEmpty { null }
        val leftJustify = match.groupValues[2] == "-"
        val widthStr = match.groupValues[3].ifEmpty { null }
        val widthPositionStr = match.groupValues[4].ifEmpty { null }
        val typeChar = match.groupValues[5]

        if (typeChar == "%") {
            sb.append("%")
        } else {
            val specContent = formatStr.substring(start, end)
            val afterPos = if (positionStr != null) 2 + positionStr.length else 1
            val afterFlag = if (leftJustify) afterPos + 1 else afterPos
            val hasAsteriskWidth = widthStr == null && widthPositionStr == null &&
                    afterFlag < specContent.length && specContent[afterFlag] == '*'

            val width: Long? = when {
                widthStr != null -> widthStr.toLong()

                widthPositionStr != null -> {
                    val wIdx = widthPositionStr.toInt() - 1
                    if (wIdx !in 0..<argCount) {
                        throw IllegalArgumentException("no argument at position ${wIdx + 1}")
                    }
                    lastConsumedIdx = wIdx
                    args[wIdx]?.toLongOrNull()
                        ?: throw IllegalArgumentException("width argument at position ${wIdx + 1} is not a number")
                }

                hasAsteriskWidth -> {
                    val wIdx = lastConsumedIdx + 1
                    if (wIdx >= argCount) {
                        throw IllegalArgumentException("no argument at position ${wIdx + 1}")
                    }
                    lastConsumedIdx = wIdx
                    args[wIdx]?.toLongOrNull()
                        ?: throw IllegalArgumentException("width argument at position ${wIdx + 1} is not a number")
                }

                else -> null
            }

            val effectiveLeftJustify = leftJustify || (width != null && width < 0)
            val effectiveWidth = width?.let { abs(it).toInt() }

            val argIdx = if (positionStr != null) positionStr.toInt() - 1 else lastConsumedIdx + 1
            if (argIdx !in 0..<argCount) {
                throw IllegalArgumentException("no argument at position ${argIdx + 1}")
            }
            lastConsumedIdx = argIdx
            val argVal = args[argIdx]

            val formatted = when (typeChar) {
                "s" -> argVal ?: ""
                "I" -> quoteIdent(argVal)
                "L" -> quoteLiteral(argVal)
                else -> throw IllegalArgumentException("Unknown format type: $typeChar")
            }

            val padded = if (effectiveWidth != null && effectiveWidth > formatted.length) {
                val padding = " ".repeat(effectiveWidth - formatted.length)
                if (effectiveLeftJustify) formatted + padding else padding + formatted
            } else {
                formatted
            }

            sb.append(padded)
        }
        lastEnd = end
    }

    sb.append(formatStr, lastEnd, formatStr.length)
    return sb.toString()
}
