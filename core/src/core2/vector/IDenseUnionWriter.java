package core2.vector;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

@SuppressWarnings("try")
public interface IDenseUnionWriter extends IVectorWriter<DenseUnionVector> {
    @Override
    DenseUnionVector getVector();

    <V extends ValueVector> IVectorWriter<V> writerForType(ArrowType arrowType);
    <V extends ValueVector> IVectorWriter<V> writerForTypeId(byte typeId);
}
