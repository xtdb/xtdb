package xtdb.util

import clojure.lang.Keyword
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.query.IKeyFn.KeyFn.*

class NormalFormTest {
    @Test
    fun `test normalForm`() {
        assertEquals("foo_bar", normalForm0("foo_bar"))
        assertEquals("foo_bar", normalForm0("foo-bar"))
        assertEquals("foobar", normalForm0("fooBar"))
        assertEquals("foo_bar", normalForm0("FOO_BAR"))
        assertEquals("foo_bar", normalForm0("FOO_bAR"))

        assertEquals("foo\$bar_baz", normalForm0("foo/bar-baz"))
        assertEquals("foo\$bar\$baz", normalForm0("foo.bar/baz"))

        assertEquals("foo\$bar\$baz", normalForm0("foo.bAr/baz"))
        assertEquals("foo\$bar\$baz", normalForm0("foo.Bar/baz"))

        assertEquals("xt\$id", normalForm0("XT\$ID"))
        assertEquals("xt\$id", normalForm0("_id"))
        assertEquals("xt\$valid_from", normalForm0("_valid_From"))
    }

    private val String.kw get() = Keyword.intern(this)

    @Test
    fun `test denormalise`() {
        assertEquals("_id", SNAKE_CASE_STRING.denormalize("xt\$id"))
        assertEquals("_valid_from", SNAKE_CASE_STRING.denormalize("xt\$valid_from"))
        assertEquals("xt/id".kw, KEBAB_CASE_KEYWORD.denormalize("xt\$id"))
        assertEquals("xt/valid-from".kw, KEBAB_CASE_KEYWORD.denormalize("xt\$valid_from"))
        assertEquals("xt/valid_from".kw, SNAKE_CASE_KEYWORD.denormalize("xt\$valid_from"))
    }
}


