package core2.vector;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;

@SuppressWarnings("try")
public interface IListWriter<V extends ValueVector> extends IVectorWriter<V> {
    <V extends ValueVector> IVectorWriter<V> getDataWriter();
}
