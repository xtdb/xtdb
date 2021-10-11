package core2.relation;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

@SuppressWarnings("try")
public interface IDenseUnionWriter extends IColumnWriter<DenseUnionVector> {
    @Override
    DenseUnionVector getVector();

    <V extends ValueVector> IColumnWriter<V> writerForType(ArrowType arrowType);
    <V extends ValueVector> IColumnWriter<V> writerForTypeId(byte typeId);
}
