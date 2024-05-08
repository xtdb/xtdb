package xtdb.vector

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.UnionVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.HashMap
import kotlin.reflect.typeOf


class SparseUnionVectorWriterTest {
    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    @Test
    fun testSimpleReadingAndWriting() {
        val longField = Field("i32", FieldType.nullable(Types.MinorType.INT.type), emptyList())
        val strField = Field("utf8", FieldType.nullable(Types.MinorType.VARCHAR.type), emptyList())
        val suvField = Field(
            "suv",
            FieldType.notNullable(ArrowType.Union(UnionMode.Sparse, intArrayOf(0, 1))),
            listOf(longField, strField))

        suvField.createVector(al).use {  srcVec ->
            val srcWriter = writerFor(srcVec)
            srcWriter.writeObject(42)
            srcWriter.writeObject("hello")
            srcWriter.syncValueCount()

            val reader = ValueVectorReader.sparseUnionVector(srcVec as UnionVector)
            assertEquals(42, reader.getObject(0))
            assertEquals("hello", reader.getObject(1))

            suvField.createVector(al).use { destVec ->
                val rowCopier =  writerFor(destVec).rowCopier(srcVec)
                for (i in 0..1)
                    rowCopier.copyRow(i)

                val destReader = ValueVectorReader.sparseUnionVector(destVec as UnionVector)
                assertEquals(42, destReader.getObject(0))
                assertEquals("hello", destReader.getObject(1))
            }
        }
    }

    @Test
    fun testAddMissingLeg() {
        val longField = Field("i32", FieldType.nullable(Types.MinorType.INT.type), emptyList())
        val suvField = Field(
            "suv",
            FieldType.notNullable(ArrowType.Union(UnionMode.Sparse, intArrayOf(0, 1))),
            listOf(longField))

        suvField.createVector(al).use {  srcVec ->
            val srcWriter = writerFor(srcVec)
            srcWriter.writeObject(42)
            srcWriter.writeObject("hello")
            srcWriter.writeObject(43)
            srcWriter.syncValueCount()

            val reader = ValueVectorReader.sparseUnionVector(srcVec as UnionVector)
            assertEquals(42, reader.getObject(0))
            assertEquals("hello", reader.getObject(1))
            assertEquals(43, reader.getObject(2))
        }
    }

    @Test
    fun testRowCopyingFromRelationToChild() {
       val longVec = Field("i64", FieldType.nullable(Types.MinorType.BIGINT.type), emptyList()).createVector(al)
       val strVec = Field("utf8", FieldType.nullable(Types.MinorType.VARCHAR.type), emptyList()).createVector(al)
       val longWriter = writerFor(longVec)
       longWriter.writeLong(42)
       longWriter.writeLong(43)
       val strWriter = writerFor(strVec)
       strWriter.writeObject("hello")
       strWriter.writeObject("world")
       val srcRelation = RelationReader.from(listOf(ValueVectorReader.from(longVec), ValueVectorReader.from(strVec)))

       // simulating something similar to an op vec here

       val putField = Field("put", FieldType.nullable(MinorType.STRUCT.type), emptyList())
       val deleteField = Field("delete", FieldType.nullable(MinorType.NULL.type), emptyList())
       val suvField = Field(
           "op",
           FieldType.notNullable(ArrowType.Union(UnionMode.Sparse, intArrayOf(0, 1))),
           listOf(putField, deleteField))

       suvField.createVector(al).use {opVec ->
           val opWriter = writerFor(opVec)
           val putWriter =  opWriter.legWriter(Keyword.intern("put"))
           val rowCopier = putWriter.rowCopier(srcRelation)
           for (i in 0..1)
               rowCopier.copyRow(i)

           val deleteWriter = opWriter.legWriter(Keyword.intern("delete"))
           deleteWriter.writeNull()

           assertEquals(2, opVec.field.children.size)

           // TODO make work
//           val opReader = ValueVectorReader.sparseUnionVector(opVec as UnionVector)
//           val r = 0..2
//           println(r.map { opReader.getObject(it)})
//           assertEquals(
//               listOf(mapOf("i64" to 42, "utf8" to "hello").toMap(HashMap()),
//                      mapOf("i64" to 43, "utf8" to "world").toMap(HashMap()), null),
//               r.map { opReader.getObject(it) })
       }

       srcRelation.close()
   }

   @Test
   fun testCopyingToPredefinedSchema() {
       val putField = Field("put", FieldType.nullable(MinorType.STRUCT.type), emptyList())
       val deleteField = Field("delete", FieldType.nullable(MinorType.NULL.type), emptyList())
       val suvField = Field(
           "op",
           FieldType.notNullable(ArrowType.Union(UnionMode.Sparse, intArrayOf(0, 1))),
           listOf(putField, deleteField))

       suvField.createVector(al).use{opVec ->
           val opWriter = writerFor(opVec)
           val putWriter =  opWriter.legWriter(Keyword.intern("put"))
           putWriter.writeObject(mapOf("id" to 42, "str" to "hello"))
           putWriter.writeObject(mapOf("id" to 43, "str" to "world"))

           val deleteWriter = opWriter.legWriter(Keyword.intern("delete"))
           deleteWriter.writeNull()

           val relReader = RelationReader.from(listOf(ValueVectorReader.sparseUnionVector(opVec as UnionVector)))

           val schema = Schema(listOf(suvField))
           val vsr = VectorSchemaRoot.create(schema, al)
           val relWriter = RootWriter(vsr)
           val copier = relWriter.rowCopier(relReader)
           for (i in 0..2)
               copier.copyRow(i)
           relWriter.syncRowCount()

           vsr.fieldVectors.first().use { opVecVSR ->
               assertEquals(2, opVecVSR.field.children.size)
               val opReader = ValueVectorReader.sparseUnionVector(opVecVSR as UnionVector)
               val r = 0..2
               println(r.map { opReader.getObject(it)})
           }
       }
   }

    @Test
    fun testInternalStructVecCreation() {
        val putField = Field("put", FieldType.nullable(MinorType.STRUCT.type), emptyList())
        val deleteField = Field("delete", FieldType.nullable(MinorType.NULL.type), emptyList())
        val suvField = Field(
            "op",
            FieldType.notNullable(ArrowType.Union(UnionMode.Sparse, intArrayOf(0, 1))),
            listOf(putField, deleteField))

        suvField.createVector(al).use{opVec ->
            val opWriter = writerFor(opVec)
            val putWriter =  opWriter.legWriter(Keyword.intern("put"))
            putWriter.writeObject(mapOf("id" to 42, "str" to "hello"))
            putWriter.writeObject(mapOf("id" to 43, "str" to "world"))

            val deleteWriter = opWriter.legWriter(Keyword.intern("delete"))
            deleteWriter.writeNull()
            opWriter.syncValueCount()

            assertEquals(2, opVec.field.children.size)
            println(opVec.toString())
            assertEquals(2, opVec.field.children.size)
        }
    }


}