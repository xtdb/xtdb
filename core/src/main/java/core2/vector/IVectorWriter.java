package core2.vector;

import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IVectorWriter<V extends ValueVector> extends AutoCloseable {
    V getVector();

    IRowCopier rowCopier(ValueVector srcVector);

    int getPosition();

    int startValue();

    void endValue();

    default IStructWriter asStruct() {
        return (IStructWriter) this;
    }

    default IListWriter<V> asList() {
        return (IListWriter<V>) this;
    }

    default IDenseUnionWriter asDenseUnion() {
        return (IDenseUnionWriter) this;
    }

    @SuppressWarnings("unchecked")
    default <I extends FieldVector, V extends ExtensionTypeVector<I>> IExtensionWriter<I, V> asExtension() {
        return (IExtensionWriter<I, V>) this;
    }

    void clear();

    @Override
    default void close() throws Exception {
        getVector().close();
    }
}
