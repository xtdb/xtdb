package xtdb.types

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.BOOL
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.NULL
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.asStructOf
import xtdb.arrow.VectorType.Companion.asUnionOf
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.unionOf

class MergeTypesTest {

    @Test
    fun `test basic mergeTypes`() {
        assertEquals(NULL, mergeTypes())

        assertEquals(UTF8, mergeTypes(UTF8, UTF8), "Same types merge ofType themselves")

        assertEquals(
            unionOf("utf8" ofType UTF8, "i64" ofType I64),
            mergeTypes(UTF8, I64),
            "Different types create unions"
        )

        assertEquals(
            unionOf("utf8" ofType UTF8, "i64" ofType I64, "f64" ofType F64),
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
            listTypeOf(unionOf("utf8" ofType UTF8, "i64" ofType I64)),
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
        val struct1 = structOf("a" ofType UTF8, "b" ofType UTF8)
        val struct2 = structOf("a" ofType UTF8, "b" ofType UTF8)

        assertEquals(
            struct1, mergeTypes(struct1, struct2),
            "Same structs merge"
        )

        val struct3 = structOf("a" ofType UTF8, "b" ofType I64)

        assertEquals(
            structOf(
                "a" ofType UTF8,
                "b".asUnionOf("utf8" ofType UTF8, "i64" ofType I64)
            ),
            mergeTypes(struct1, struct3),
            "Structs with different field types create union in differing fields"
        )
    }

    @Test
    fun `test struct merging with different fields`() {
        assertEquals(
            structOf(
                "a" ofType maybe(UTF8),
                "b" ofType UTF8,
                "c" ofType maybe(I64),
            ),
            mergeTypes(
                structOf("a" ofType UTF8, "b" ofType UTF8),
                structOf("b" ofType UTF8, "c" ofType I64)
            ),
            "Struct merging with different fields makes missing fields nullable"
        )
    }

    @Test
    fun `test union with struct and float`() {
        val unionWithStruct = unionOf("f64" ofType F64, "struct".asStructOf("a" ofType I64))
        val justStruct = structOf("a" ofType UTF8)

        val expected = unionOf(
            "f64" ofType F64,
            "struct".asStructOf("a".asUnionOf("i64" ofType I64, "utf8" ofType UTF8))
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
            unionOf("i64" ofType I64, "utf8" ofType UTF8, "null" ofType NULL),
            mergeTypes(NULL, I64, UTF8),
            "Null with multiple other types creates union"
        )

        assertEquals(
            unionOf("i64" ofType maybe(I64), "utf8" ofType UTF8),
            mergeTypes(maybe(I64), UTF8),
            "Nullable legs are preserved"
        )
    }

    @Test
    fun `test set types`() {
        val setInt64 = VectorType.setTypeOf(I64)
        val setUtf8 = VectorType.setTypeOf(UTF8)

        assertEquals(
            setInt64, mergeTypes(setInt64, setInt64),
            "Same set types merge"
        )

        val expectedMergedSet = VectorType.setTypeOf(unionOf("i64" ofType I64, "utf8" ofType UTF8))
        assertEquals(
            expectedMergedSet, mergeTypes(setInt64, setUtf8),
            "Different set element types create union"
        )
    }

    @Test
    fun `test complex nested struct merging`() {
        val struct0 = structOf(
            "a" ofType I64,
            "b".asStructOf("c" ofType UTF8, "d" ofType UTF8)
        )
        val struct1 = structOf(
            "a" ofType BOOL,
            "b" ofType UTF8
        )

        val expected = structOf(
            "a".asUnionOf("i64" ofType I64, "bool" ofType BOOL),
            "b".asUnionOf(
                "utf8" ofType UTF8,
                "struct".asStructOf("c" ofType UTF8, "d" ofType UTF8),
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
            unionOf("f64" ofType F64, "i64" ofType I64, "null" ofType NULL),
            mergeTypes(F64, NULL, I64),
            "Multiple types with null creates union including null"
        )
    }

    @Test
    fun `test no struct squashing`() {
        val nestedStruct = structOf("foo".asStructOf("bibble" ofType BOOL))
        assertEquals(
            nestedStruct, mergeTypes(nestedStruct),
            "Nested structs don't get flattened"
        )

        val mixedStruct = structOf("foo" ofType UTF8, "bar" ofType I64)

        assertEquals(
            structOf(
                "foo".asUnionOf(
                    "utf8" ofType UTF8,
                    "struct".asStructOf("bibble" ofType BOOL),
                ),
                "bar" ofType maybe(I64)
            ),
            mergeTypes(nestedStruct, mixedStruct),
            "Nested struct merging with other fields preserves structure"
        )
    }

}