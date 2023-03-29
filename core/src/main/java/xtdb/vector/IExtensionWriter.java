package xtdb.vector;

import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;

@SuppressWarnings("try")
public interface IExtensionWriter<I extends FieldVector, V extends ExtensionTypeVector<I>> extends IVectorWriter<V> {
    V getVector();
    IVectorWriter<I> getUnderlyingWriter();
}
