package xtdb.util

import java.lang.Integer.toUnsignedString
import java.lang.Long.toUnsignedString
import java.nio.ByteBuffer
import kotlin.math.max
import kotlin.math.min

object StringUtil {
    /**
     * Returns the 1-based position of the given (needle) bytes in the haystack. 0 if not found.
     * This is equivalent to sql POSITION on binary.
     */
    @JvmStatic
    fun SqlBinPosition(needle: ByteBuffer, haystack: ByteBuffer): Int {
        if (needle.remaining() == 0) return 1
        var i = 0
        var j = 0
        while (true) {
            if (j == needle.remaining()) return i + 1
            if (i + j == haystack.remaining()) return 0
            if (haystack[i + j] == needle[j]) j++ else { i++; j = 0 }
        }
    }

    @JvmStatic
    fun isUtf8Char(b: Byte) = (b.toInt() and 0xc0) != 0x80

    /**
     * In our SQL dialect &amp; arrow we operate always on utf8.
     * Java strings are utf-16 backed, and java chars cannot represent all utf-8 characters in one char - therefore
     * we do not use string length or anything like it, so that one character == one code point.
     */
    @JvmStatic
    @JvmOverloads
    fun utf8Length(buf: ByteBuffer, start: Int = 0, end: Int = buf.remaining()): Int {
        var len = 0
        for (i in start until end)
            if (isUtf8Char(buf[i])) len++
        return len
    }

    /**
     * Returns the 1-based codepoint position of the needle in the haystack. 0 if not found.
     * This is equivalent to sql POSITION on characters.
     */
    @JvmStatic
    fun sqlUtf8Position(needle: ByteBuffer, haystack: ByteBuffer): Int {
        val bpos = SqlBinPosition(needle, haystack)
        if (bpos == 0) return 0
        return utf8Length(haystack, 0, bpos - 1) + 1
    }

    @Suppress("NAME_SHADOWING")
    @JvmStatic
    fun sqlUtf8Substring(target: ByteBuffer, pos: Int, len: Int, useLen: Boolean): ByteBuffer {
        var len = len
        require(!(useLen && len < 0)) { "Negative substring length" }

        val zeroPos = pos - 1
        len = if (zeroPos < 0) max(0.0, (len + zeroPos).toDouble()).toInt() else len

        if (useLen && len == 0) return ByteBuffer.allocate(0)

        val startCodepoint = max(zeroPos.toDouble(), 0.0).toInt()
        var startIndex = 0

        var cp = -1
        run {
            var i = target.position()
            while (i < target.remaining() && cp < startCodepoint) {
                if (isUtf8Char(target[i])) {
                    cp++
                }
                startIndex = i
                i++
            }
        }

        return when {
            cp < startCodepoint -> ByteBuffer.allocate(0)
            !useLen -> target.duplicate().apply { position(startIndex) }
            else -> {
                var charsConsumed = 0
                for (i in startIndex until target.remaining()) {
                    if (charsConsumed == len && isUtf8Char(target[i]))
                        return target.duplicate().apply { position(startIndex); limit(i) }

                    if (isUtf8Char(target[i])) charsConsumed++
                }

                target.duplicate().apply { position(startIndex) }
            }
        }
    }

    @JvmStatic
    @Suppress("NAME_SHADOWING")
    fun sqlBinSubstring(target: ByteBuffer, pos: Int, len: Int, useLen: Boolean): ByteBuffer {
        var len = len

        require(!(useLen && len < 0)) { "Negative substring length" }

        val zeroPos = pos - 1
        len = if (zeroPos < 0) max(0.0, (len + zeroPos).toDouble()).toInt() else len
        val startIndex = max(0.0, zeroPos.toDouble()).toInt()

        return when {
            useLen && len == 0 -> ByteBuffer.allocate(0)
            startIndex >= target.remaining() -> ByteBuffer.allocate(0)
            !useLen -> target.slice().apply { position(startIndex) }
            else -> {
                val limit = min((startIndex + len).toDouble(), target.remaining().toDouble()).toInt()

                target.slice().apply { position(startIndex); limit(limit) }
            }
        }
    }

    @JvmStatic
    fun sqlUtf8Overlay(target: ByteBuffer, placing: ByteBuffer, start: Int, replaceLength: Int): ByteBuffer {
        val s1 = sqlUtf8Substring(target, 1, start - 1, true)
        val s2 = sqlUtf8Substring(target, start + replaceLength, -1, false)

        return ByteBuffer.allocate(s1.remaining() + s2.remaining() + placing.remaining()).apply {
            put(s1)
            put(placing.duplicate())
            put(s2)
            position(0)
        }
    }

    @JvmStatic
    fun sqlBinOverlay(target: ByteBuffer, placing: ByteBuffer, start: Int, replaceLength: Int): ByteBuffer {
        val s1 = sqlBinSubstring(target, 1, start - 1, true)
        val s2 = sqlBinSubstring(target, start + replaceLength, -1, false)

        return ByteBuffer.allocate(s1.remaining() + s2.remaining() + placing.remaining()).apply {
            put(s1)
            put(placing.duplicate())
            put(s2)
            position(0)
        }
    }

    @Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")
    @JvmStatic
    fun String.parseCString(): String =
        // Java has String.translateEscapes, but it doesn't handle \x or \u

        replace(Regex("\\\\([0-7]{1,3}|x(\\p{XDigit}{1,2})|u(\\p{XDigit}{1,4})|U(\\p{XDigit}{1,8})|[^0-7xuU])")) {
            val s = it.value
            when (s[1]) {
                'x', 'u', 'U' -> Character.toChars(s.substring(2).toUInt(16).toInt()).concatToString()
                else -> (s as java.lang.String).translateEscapes()
            }
        }

    val Long.asLexHex: String
        get() {
            val body = toUnsignedString(this, 16)
            return "${toUnsignedString(body.length - 1, 16)}$body"
        }
}
