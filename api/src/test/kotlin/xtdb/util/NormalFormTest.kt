package xtdb.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NormalFormTest {
    @Test
    fun `test normalForm`() {
        assertEquals("foo_bar", normalForm("foo_bar"))
        assertEquals("foo_bar", normalForm("foo-bar"))
        assertEquals("foo_bar", normalForm("fooBar"))
        assertEquals("foo_bar", normalForm("FOO_BAR"))
        assertEquals("foo_b_ar", normalForm("FOO_bAR"))

        assertEquals("foo\$bar_baz", normalForm("foo/bar-baz"))
        assertEquals("foo\$bar\$baz", normalForm("foo.bar/baz"))

        assertEquals("foo\$b_ar\$baz", normalForm("foo.bAr/baz"))
        assertEquals("foo\$bar\$baz", normalForm("foo.Bar/baz"))

        assertEquals("xt\$id", normalForm("XT\$ID"))
    }
}


