package core2.relation;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

@SuppressWarnings("try")
public interface IColumnWriter<V extends ValueVector> extends AutoCloseable {
    V getVector();

    int appendIndex();

    int appendIndex(int parentIndex);

    IRowAppender rowAppender(IColumnReader<?> sourceColumn);

    default <V extends ValueVector> IColumnWriter<V> writerForName(String columnName) {
        throw new UnsupportedOperationException();
    }

    default <V extends ValueVector> IColumnWriter<V> writerForTypeId(byte typeId) {
        throw new UnsupportedOperationException();
    }

    default <V extends ValueVector> IColumnWriter<V> writerForType(ArrowType arrowType) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws Exception {
        getVector().close();
    }
}
