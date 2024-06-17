package xtdb.util

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.util.StringUtil.parseCString

class StringUtilTest {
    @Test
    fun `test parseCString`() {
        assertEquals("\n", "\\n".parseCString())
        assertEquals("\r", "\\r".parseCString())
        assertEquals("\t", "\\t".parseCString())
        assertEquals(" ", "\\40".parseCString())
        assertEquals(" ", "\\x20".parseCString())
        assertEquals(" ", "\\u0020".parseCString())
        assertEquals("😁", "\\U0001F601".parseCString())
        assertEquals("\\", "\\\\".parseCString())
        assertEquals("'", "\\\'".parseCString())
    }
}
