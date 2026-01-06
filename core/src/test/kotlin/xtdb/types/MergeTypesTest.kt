package xtdb.types

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.VectorType.Companion.BOOL
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.NULL
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.asStructOf
import xtdb.arrow.VectorType.Companion.asUnionOf
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.setTypeOf
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.unionOf

class MergeTypesTest {

    @Test
    fun `test basic mergeTypes`() {
        assertEquals(NULL, mergeTypes())

        assertEquals(UTF8, mergeTypes(UTF8, UTF8), "Same types merge ofType themselves")

        assertEquals(
            unionOf("utf8" to UTF8, "i64" to I64),
            mergeTypes(UTF8, I64),
            "Different types create unions"
        )

        assertEquals(
            unionOf("utf8" to UTF8, "i64" to I64, "f64" to F64),
            mergeTypes(UTF8, I64, F64),
            "Multiple different types create unions"
        )
    }

    @Test
    fun `test merges list types`() {
        val utf8List = listTypeOf(UTF8)
        val i64List = listTypeOf(I64)
        val nullList = listTypeOf(NULL)

        assertEquals(utf8List, mergeTypes(utf8List, utf8List), "Same list types merge")

        assertEquals(
            listTypeOf(unionOf("utf8" to UTF8, "i64" to I64)),
            mergeTypes(utf8List, i64List),
            "Different list element types create union"
        )

        assertEquals(
            listTypeOf(maybe(I64)),
            mergeTypes(nullList, i64List),
            "List with null element creates nullable element type"
        )
    }

    @Test
    fun `test merges struct types`() {
        val struct1 = structOf("a" to UTF8, "b" to UTF8)
        val struct2 = structOf("a" to UTF8, "b" to UTF8)

        assertEquals(
            struct1, mergeTypes(struct1, struct2),
            "Same structs merge"
        )

        val struct3 = structOf("a" to UTF8, "b" to I64)

        assertEquals(
            structOf(
                "a" to UTF8,
                "b".asUnionOf("utf8" to UTF8, "i64" to I64)
            ),
            mergeTypes(struct1, struct3),
            "Structs with different field types create union in differing fields"
        )
    }

    @Test
    fun `test struct merging with different fields`() {
        assertEquals(
            structOf(
                "a" to maybe(UTF8),
                "b" to UTF8,
                "c" to maybe(I64),
            ),
            mergeTypes(
                structOf("a" to UTF8, "b" to UTF8),
                structOf("b" to UTF8, "c" to I64)
            ),
            "Struct merging with different fields makes missing fields nullable"
        )
    }

    @Test
    fun `test union with struct and float`() {
        val unionWithStruct = unionOf("f64" to F64, "struct".asStructOf("a" to I64))
        val justStruct = structOf("a" to UTF8)

        val expected = unionOf(
            "f64" to F64,
            "struct".asStructOf("a".asUnionOf("i64" to I64, "utf8" to UTF8))
        )
        assertEquals(
            expected,
            mergeTypes(unionWithStruct, justStruct),
            "Union with struct merges struct fields recursively"
        )
    }

    @Test
    fun `test null behaviour`() {
        assertEquals(
            NULL, mergeTypes(NULL),
            "Single null remains null"
        )

        assertEquals(
            NULL, mergeTypes(NULL, NULL),
            "Multiple nulls remain null"
        )

        assertEquals(
            maybe(I64), mergeTypes(NULL, I64),
            "Null with one other type creates nullable type"
        )

        assertEquals(
            unionOf("i64" to I64, "utf8" to UTF8, "null" to NULL),
            mergeTypes(NULL, I64, UTF8),
            "Null with multiple other types creates union"
        )

        assertEquals(
            unionOf("i64" to maybe(I64), "utf8" to UTF8),
            mergeTypes(maybe(I64), UTF8),
            "Nullable legs are preserved"
        )
    }

    @Test
    fun `test set types`() {
        val setInt64 = setTypeOf(I64)
        val setUtf8 = setTypeOf(UTF8)

        assertEquals(
            setInt64, mergeTypes(setInt64, setInt64),
            "Same set types merge"
        )

        val expectedMergedSet = setTypeOf(unionOf("i64" to I64, "utf8" to UTF8))
        assertEquals(
            expectedMergedSet, mergeTypes(setInt64, setUtf8),
            "Different set element types create union"
        )
    }

    @Test
    fun `test complex nested struct merging`() {
        val struct0 = structOf(
            "a" to I64,
            "b".asStructOf("c" to UTF8, "d" to UTF8)
        )
        val struct1 = structOf(
            "a" to BOOL,
            "b" to UTF8
        )

        val expected = structOf(
            "a".asUnionOf("i64" to I64, "bool" to BOOL),
            "b".asUnionOf(
                "utf8" to UTF8,
                "struct".asStructOf("c" to UTF8, "d" to UTF8),
            )
        )
        assertEquals(
            expected, mergeTypes(struct0, struct1),
            "Nested struct merging with simple field creates union"
        )
    }

    @Test
    fun `test multiple nulls with different types`() {
        assertEquals(
            unionOf("f64" to F64, "i64" to I64, "null" to NULL),
            mergeTypes(F64, NULL, I64),
            "Multiple types with null creates union including null"
        )
    }

    @Test
    fun `test no struct squashing`() {
        val nestedStruct = structOf("foo".asStructOf("bibble" to BOOL))
        assertEquals(
            nestedStruct, mergeTypes(nestedStruct),
            "Nested structs don't get flattened"
        )

        val mixedStruct = structOf("foo" to UTF8, "bar" to I64)

        assertEquals(
            structOf(
                "foo".asUnionOf(
                    "utf8" to UTF8,
                    "struct".asStructOf("bibble" to BOOL),
                ),
                "bar" to maybe(I64)
            ),
            mergeTypes(nestedStruct, mixedStruct),
            "Nested struct merging with other fields preserves structure"
        )
    }

}