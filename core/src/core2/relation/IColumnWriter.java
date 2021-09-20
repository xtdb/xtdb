package core2.relation;

import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IColumnWriter<V extends ValueVector> extends AutoCloseable {
    IRowAppender rowAppender(IColumnReader<?> sourceColumn);

    IColumnReader<V> read();

    void clear();
}
