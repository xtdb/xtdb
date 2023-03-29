package xtdb.vector;

import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IListWriter<V extends ValueVector> extends IVectorWriter<V> {
    <V extends ValueVector> IVectorWriter<V> getDataWriter();
}
