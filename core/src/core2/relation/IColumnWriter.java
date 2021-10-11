package core2.relation;

import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IColumnWriter<V extends ValueVector> extends AutoCloseable {
    V getVector();

    <R extends ValueVector> IRowCopier<R, V> rowCopier(IColumnReader<R> sourceColumn);

    int getPosition();

    int startValue();
    void endValue();

    default IStructWriter asStruct() {
        throw new ClassCastException("not a struct");
    }

    default IListWriter asList() {
        throw new ClassCastException("not a list");
    }

    default IDenseUnionWriter asDenseUnion() {
        throw new ClassCastException("not a dense union");
    }

    void clear();

    @Override
    default void close() throws Exception {
        getVector().close();
    }
}
