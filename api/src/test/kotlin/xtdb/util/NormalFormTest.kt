package xtdb.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NormalFormTest {
    @Test
    fun `test normalForm`() {
        assertEquals("foo_bar", normalForm0("foo_bar"))
        assertEquals("foo_bar", normalForm0("foo-bar"))
        assertEquals("foo_bar", normalForm0("fooBar"))
        assertEquals("foo_bar", normalForm0("FOO_BAR"))
        assertEquals("foo_b_ar", normalForm0("FOO_bAR"))

        assertEquals("foo\$bar_baz", normalForm0("foo/bar-baz"))
        assertEquals("foo\$bar\$baz", normalForm0("foo.bar/baz"))

        assertEquals("foo\$b_ar\$baz", normalForm0("foo.bAr/baz"))
        assertEquals("foo\$bar\$baz", normalForm0("foo.Bar/baz"))

        assertEquals("xt\$id", normalForm0("XT\$ID"))
    }
}


