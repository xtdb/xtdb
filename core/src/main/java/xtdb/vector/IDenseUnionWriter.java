package xtdb.vector;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.types.pojo.Field;

@SuppressWarnings("try")
public interface IDenseUnionWriter extends IVectorWriter<DenseUnionVector> {
    @Override
    DenseUnionVector getVector();

    <V extends ValueVector> IVectorWriter<V> writerForType(Object colType);
    <V extends ValueVector> IVectorWriter<V> writerForTypeId(byte typeId);
    byte registerNewType(Field field);
}
