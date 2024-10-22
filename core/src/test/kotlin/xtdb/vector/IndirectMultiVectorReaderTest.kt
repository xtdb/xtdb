package xtdb.vector

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorPosition
import xtdb.arrow.ValueReader
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.toArrowType
import xtdb.toLeg
import org.apache.arrow.vector.types.Types.MinorType.DENSEUNION as DENSEUNION_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.Bool.INSTANCE as BOOL_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE
import org.apache.arrow.vector.types.pojo.ArrowType.List.INSTANCE as LIST_TYPE

private val I32 = FieldType.notNullable(Types.MinorType.INT.type)

fun <T> cycle(list: List<T>): Sequence<T> {
    return sequence {
        while (true) {
            yieldAll(list)
        }
    }
}

class IndirectMultiVectorReaderTest {
    private lateinit var alloc: RootAllocator

    @BeforeEach
    fun setUp() {
        alloc = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        alloc.close()
    }

    @Test
    fun testMonomorphicSimpleVectors() {
        val intVec1 = IntVector("my-int", alloc)
        val intVec2 = IntVector("my-int", alloc)
        for (i in 0..4) {
            if (i % 2 == 0) intVec1.setSafe(i / 2, i)
            else intVec2.setSafe(i / 2, i)
        }
        intVec1.valueCount = 2
        intVec2.valueCount = 2

        val rdr1 = ValueVectorReader.intVector(intVec1)
        val rdr2 = ValueVectorReader.intVector(intVec2)
        val indirectRdr = IndirectMultiVectorReader(
            listOf(rdr1, rdr2),
            selection(intArrayOf(0, 1, 0, 1)),
            selection(intArrayOf(0, 0, 1, 1))
        )
        val r = 0..3
        assertEquals(r.toList(), r.map { indirectRdr.getInt(it) })

        val pos = VectorPosition.build(0)
        val valueRdr = indirectRdr.valueReader(pos)
        assertEquals(r.toList(), r.map { valueRdr.readInt().also { pos.getPositionAndIncrement() } })

        val resVec = IntVector("my-int", alloc)
        val vectorWriter = writerFor(resVec)
        val rowCopier = indirectRdr.rowCopier(vectorWriter)
        r.map { rowCopier.copyRow(it) }
        vectorWriter.syncValueCount()
        assertEquals(r.toList(), r.map { resVec[it] })

        rdr1.close()
        rdr2.close()
        vectorWriter.close()
    }

    private fun readMaps(valueReader: ValueReader): Any? {
        return when (val o = valueReader.readObject()) {
            is Map<*, *> -> o.mapValues { readMaps(it.value as ValueReader) }
            else -> o
        }
    }

    @Test
    fun testMonomorphicStructVectors() {
        val fooField = Field("foo", FieldType.notNullable(BOOL_TYPE), null)
        val barField = Field("bar", FieldType.notNullable(BOOL_TYPE), null)
        val structField = Field("my-struct", FieldType(false, STRUCT_TYPE, null, null), listOf(fooField, barField))
        val structVec1 = structField.createVector(alloc) as StructVector
        val structVec2 = structField.createVector(alloc) as StructVector
        val structVec1Writer = StructVectorWriter(structVec1, null)
        val structVec2Writer = StructVectorWriter(structVec2, null)

        val m1 = mapOf("foo" to false, "bar" to true)
        val m2 = mapOf("foo" to true, "bar" to false)

        for (i in 0..4) {
            if (i % 2 == 0) structVec1Writer.writeObject(m1)
            else structVec2Writer.writeObject(m2)
        }
        structVec1Writer.syncValueCount()
        structVec2Writer.syncValueCount()

        val rdr1 = ValueVectorReader.structVector(structVec1)
        val rdr2 = ValueVectorReader.structVector(structVec2)
        val indirectRdr = IndirectMultiVectorReader(
            listOf(rdr1, rdr2),
            selection(intArrayOf(0, 1, 0, 1)),
            selection(intArrayOf(0, 0, 1, 1))
        )
        val r = 0..3
        val expected = cycle(listOf(m1, m2)).take(4).toList()
        assertEquals(expected, r.map { indirectRdr.getObject(it) })

        val pos = VectorPosition.build(0)
        val valueRdr = indirectRdr.valueReader(pos)
        assertEquals(expected, r.map { readMaps(valueRdr).also { pos.getPositionAndIncrement() } })

        val resVec = structField.createVector(alloc) as StructVector
        val vectorWriter = writerFor(resVec)
        val rowCopier = indirectRdr.rowCopier(vectorWriter)
        r.map { rowCopier.copyRow(it) }
        vectorWriter.syncValueCount()
        val resRdr = ValueVectorReader.structVector(resVec)
        assertEquals(expected, r.map { resRdr.getObject(it) })

        rdr1.close()
        rdr2.close()
        vectorWriter.close()
    }

    @Test
    fun testPolymorphicSimpleVectors() {
        val intVec = IntVector("my-int-or-str", alloc)
        val stringVec = VarCharVector("my-int-or-str", alloc)
        val stringVecWriter = writerFor(stringVec)
        intVec.setSafe(0, 0)
        intVec.setSafe(1, 1)
        stringVecWriter.writeObject("first")
        stringVecWriter.writeObject("second")
        intVec.valueCount = 2
        stringVecWriter.syncValueCount()

        val rdr1 = ValueVectorReader.intVector(intVec)
        val rdr2 = ValueVectorReader.varCharVector(stringVec)
        val indirectRdr = IndirectMultiVectorReader(
            listOf(rdr1, rdr2),
            selection(intArrayOf(0, 1, 0, 1)),
            selection(intArrayOf(0, 0, 1, 1))
        )
        val r = 0..3
        val expected = listOf(0, "first", 1, "second")
        assertEquals(expected, r.map { indirectRdr.getObject(it) })

        val pos = VectorPosition.build(0)
        val valueRdr = indirectRdr.valueReader(pos)
        assertEquals(expected, r.map { valueRdr.readObject().also { pos.getPositionAndIncrement() } })

        val duvField = Field("my-duv", FieldType(false, DENSEUNION_TYPE.type, null, null), null)
        val resVec = duvField.createVector(alloc) as DenseUnionVector
        val vectorWriter = writerFor(resVec)
        val rowCopier = indirectRdr.rowCopier(vectorWriter)
        r.map { rowCopier.copyRow(it) }
        vectorWriter.syncValueCount()
        val resRdr = ValueVectorReader.denseUnionVector(resVec)
        assertEquals(expected, r.map { resRdr.getObject(it) })

        rdr1.close()
        rdr2.close()
        vectorWriter.close()
    }

    @Test
    fun testPolymorphicSimpleAndComplexVectors() {
        val intVec = IntVector("my-int-or-str", alloc)
        val stringVec = VarCharVector("my-int-or-str", alloc)
        val stringVecWriter = writerFor(stringVec)
        val duvField = Field("my-duv", FieldType(false, DENSEUNION_TYPE.type, null, null), null)
        val duvVec = duvField.createVector(alloc) as DenseUnionVector
        val duvVectorWriter = writerFor(duvVec)


        intVec.setSafe(0, 0)
        stringVecWriter.writeObject("first")
        duvVectorWriter.writeObject(2)
        intVec.setSafe(1, 3)
        stringVecWriter.writeObject("fourth")
        duvVectorWriter.writeObject("fifth")

        intVec.valueCount = 2
        stringVecWriter.syncValueCount()
        duvVectorWriter.syncValueCount()

        val rdr1 = ValueVectorReader.intVector(intVec)
        val rdr2 = ValueVectorReader.varCharVector(stringVec)
        val rdr3 = ValueVectorReader.denseUnionVector(duvVec)
        val indirectRdr = IndirectMultiVectorReader(
            listOf(rdr1, rdr2, rdr3),
            selection(intArrayOf(0, 1, 2, 0, 1, 2)),
            selection(intArrayOf(0, 0, 0, 1, 1, 1))
        )
        val r = 0..5
        val expected = listOf(0, "first", 2, 3, "fourth", "fifth")
        assertEquals(expected, r.map { indirectRdr.getObject(it) })

        val pos = VectorPosition.build(0)
        val valueRdr = indirectRdr.valueReader(pos)
        assertEquals(expected, r.map { valueRdr.readObject().also { pos.getPositionAndIncrement() } })

        val resVec = duvField.createVector(alloc) as DenseUnionVector
        val vectorWriter = writerFor(resVec)
        val rowCopier = indirectRdr.rowCopier(vectorWriter)
        r.map { rowCopier.copyRow(it) }
        vectorWriter.syncValueCount()
        val resRdr = ValueVectorReader.denseUnionVector(resVec)
        assertEquals(expected, r.map { resRdr.getObject(it) })

        rdr1.close()
        rdr2.close()
        rdr3.close()
        vectorWriter.close()
    }

    @Test
    fun testAbsentVectors() {
        val duvField = Field("my-duv", FieldType(false, DENSEUNION_TYPE.type, null, null), null)
        val duvVec1 = duvField.createVector(alloc) as DenseUnionVector
        val duvVec2 = duvField.createVector(alloc) as DenseUnionVector
        val duvVectorWriter1 = writerFor(duvVec1)
        val duvVectorWriter2 = writerFor(duvVec2)

        duvVectorWriter1.writeObject(0)
        duvVectorWriter2.writeObject("first")
        duvVectorWriter1.populateWithAbsents(2)
        duvVectorWriter2.writeObject(3)
        duvVectorWriter1.writeObject("fourth")
        duvVectorWriter2.populateWithAbsents(3)

        val rdr1 = ValueVectorReader.denseUnionVector(duvVec1)
        val rdr2 = ValueVectorReader.denseUnionVector(duvVec2)
        val indirectRdr = IndirectMultiVectorReader(
            listOf(rdr1, rdr2),
            selection(intArrayOf(0, 1, 0, 1, 0, 1)),
            selection(intArrayOf(0, 0, 1, 1, 2, 2))
        )
        val r = 0..5
        val expected = listOf(0, "first", null, 3, "fourth", null)
        assertEquals(expected, r.map {
            if (indirectRdr.isNull(it)) null
            else indirectRdr.getObject(it)
        })

        val pos = VectorPosition.build(0)
        val valueRdr = indirectRdr.valueReader(pos)
        assertEquals(expected, r.map {
            val res =
                if (valueRdr.isNull) null
                else valueRdr.readObject()
            pos.getPositionAndIncrement()
            res
        })

        val resVec = duvField.createVector(alloc) as DenseUnionVector
        val vectorWriter = writerFor(resVec)
        val rowCopier = indirectRdr.rowCopier(vectorWriter)
        r.map { rowCopier.copyRow(it) }
        vectorWriter.syncValueCount()
        val resRdr = ValueVectorReader.denseUnionVector(resVec)
        assertEquals(expected, r.map { resRdr.getObject(it) })

        rdr1.close()
        rdr2.close()
        vectorWriter.close()
    }

    @Test
    fun testSingleLeggedDUVs() {
        val duvField = Field("my-duv", FieldType(false, DENSEUNION_TYPE.type, null, null), null)
        val duvVec1 = duvField.createVector(alloc) as DenseUnionVector
        val duvVectorWriter1 = writerFor(duvVec1)

        duvVectorWriter1.writeObject(0)
        duvVectorWriter1.writeObject(1)

        val rdr1 = ValueVectorReader.denseUnionVector(duvVec1)
        val indirectRdr = IndirectMultiVectorReader(
            listOf(rdr1),
            selection(intArrayOf(0, 0)),
            selection(intArrayOf(0, 1))
        )

        val r = 0..1
        val expected = listOf(0, 1)
        assertEquals(expected, r.map { indirectRdr.getObject(it) })

        val pos = VectorPosition.build(0)
        val valueRdr = indirectRdr.valueReader(pos)
        assertEquals(expected, r.map {
            val res = valueRdr.readInt()
            pos.getPositionAndIncrement()
            res
        })

        rdr1.close()
    }


    @Test
    fun testListElementReader () {
        val listField = Field("my-list", FieldType(false, LIST_TYPE, null), listOf(Field("\$data\$", I32, null)))
        val listVec1 = listField.createVector(alloc) as ListVector
        val listVec2 = listField.createVector(alloc) as ListVector
        val listVectorWriter1 = writerFor(listVec1)
        val listVectorWriter2 = writerFor(listVec2)

        listVectorWriter1.writeObject(listOf(0, 1, 2))
        listVectorWriter1.writeObject(listOf(3, 4, 5))

        listVectorWriter2.writeObject(listOf(6, 7))
        listVectorWriter2.writeObject(listOf(8, 9))

        val rdr1 = ValueVectorReader.listVector(listVec1)
        val rdr2 = ValueVectorReader.listVector(listVec2)

        // This represents the vector [[0, 1, 2], [6, 7], [3, 4, 5], [8, 9]]
        val indirectRdr = IndirectMultiVectorReader(
            listOf(rdr1, rdr2),

            selection(intArrayOf(0, 1, 0, 1)),
            selection(intArrayOf(0, 0, 1, 1))
        )

        assertEquals(listOf(0, 1, 2), indirectRdr.getObject(0))
        assertEquals(listOf(8, 9), indirectRdr.getObject(3))

        assertEquals(3, indirectRdr.getListStartIndex(1))
        assertEquals(5, indirectRdr.getListStartIndex(2))

        assertEquals(3, indirectRdr.getListCount(0))
        assertEquals(2, indirectRdr.getListCount(1))

        // The elementRdr should contain [0, 1, 2, 6, 7, 3, 4, 5, 8, 9]a
        val listElementRdr = indirectRdr.listElementReader()

        val r = 0..9
        assertEquals(listOf(0, 1, 2, 6, 7, 3, 4, 5, 8, 9) , r.map { listElementRdr.getInt(it) }.toList())

        indirectRdr.close()
        rdr1.close()
        rdr2.close()
    }

    @Test
    fun testListElementReaderWithPolymophicUnderlyingVectors () {
        val listField = Field("my-list", FieldType(false, LIST_TYPE, null), listOf(Field("\$data\$", I32, null)))
        val listVec = listField.createVector(alloc) as ListVector
        val intVec = IntVector("my-int", alloc)

        val listWriter = writerFor(listVec)
        val intWriter = writerFor(intVec)

        listWriter.writeObject(listOf(0, 1, 2))
        listWriter.writeObject(listOf(3, 4, 5))

        intWriter.writeInt(0)
        intWriter.writeInt(1)

        val rdr1 = ValueVectorReader.listVector(listVec)
        val rdr2 = ValueVectorReader.intVector(intVec)

        // This represents the vector [[0, 1, 2], 0, [3, 4, 5], 1]
        val indirectRdr = IndirectMultiVectorReader(
            listOf(rdr1, rdr2),

            selection(intArrayOf(0, 1, 0, 1)),
            selection(intArrayOf(0, 0, 1, 1))
        )

        assertEquals(listOf(0, 1, 2), indirectRdr.getObject(0))
        assertEquals(1, indirectRdr.getObject(3))

        val listRdr = indirectRdr.legReader("list")

        assertEquals(0, listRdr.getListStartIndex(0))
        assertEquals(3, listRdr.getListStartIndex(1))

        val listElementRdr = listRdr.listElementReader()

        val r = 0 until listElementRdr.valueCount()
        assertEquals(listOf(0, 1, 2, 3, 4, 5) , r.map { listElementRdr.getInt(it) }.toList())

        indirectRdr.close()
        rdr1.close()
        rdr2.close()
    }


}