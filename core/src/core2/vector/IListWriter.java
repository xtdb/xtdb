package core2.vector;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;

@SuppressWarnings("try")
public interface IListWriter extends IVectorWriter<ListVector> {
    @Override
    ListVector getVector();

    <V extends ValueVector> IVectorWriter<V> getDataWriter();
}
