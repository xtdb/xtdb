package xtdb.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xtdb.api.Xtdb;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparseUnionVectorTest {
   private BufferAllocator al;

   @BeforeEach
   void setUp() {
      al = new RootAllocator();
   }

   @AfterEach
   void tearDown() {
      al.close();
   }

   @Test
   void testPrintIssue(){
       Field putField = new Field("put", FieldType.nullable(Types.MinorType.STRUCT.getType()), Collections.emptyList());
       Field deleteField = new Field("delete", FieldType.nullable(Types.MinorType.NULL.getType()), Collections.emptyList());
       Field suvField = new Field(
               "op",
               FieldType.notNullable(new ArrowType.Union(UnionMode.Sparse, new int[]{0, 1})),
               Arrays.asList(putField, deleteField));

       UnionVector suvVec =  (UnionVector) suvField.createVector(al);
       StructVector putVec =  (StructVector) suvVec.getChild("put");
       FieldVector deleteVec =  suvVec.getChild("delete");

       BigIntVector longVec = (BigIntVector) putVec.addOrGet("i64", FieldType.notNullable(Types.MinorType.BIGINT.getType()),FieldVector.class);
       longVec.setSafe(0, 42);
       suvVec.setTypeId(0, (byte)0);

       deleteVec.setNull(0);
       suvVec.setTypeId(0, (byte)1);
       suvVec.setValueCount(2);

       assertEquals(2, suvVec.getField().getChildren().size());

       System.out.println(suvVec.toString());

       assertEquals(2, suvVec.getField().getChildren().size());

       suvVec.close();
   }
}
