package core2.vector;

import core2.types.LegType;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;

@SuppressWarnings("try")
public interface IDenseUnionWriter extends IVectorWriter<DenseUnionVector> {
    @Override
    DenseUnionVector getVector();

    <V extends ValueVector> IVectorWriter<V> writerForType(LegType legType);
    <V extends ValueVector> IVectorWriter<V> writerForTypeId(byte typeId);
}
