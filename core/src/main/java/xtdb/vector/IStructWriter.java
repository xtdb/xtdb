package xtdb.vector;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

@SuppressWarnings("try")
public interface IStructWriter extends IVectorWriter<StructVector> {
    @Override
    StructVector getVector();

    <V extends ValueVector> IVectorWriter<V> writerForName(String columnName);
}
