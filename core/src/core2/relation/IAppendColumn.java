package core2.relation;

import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IAppendColumn<V extends ValueVector> extends AutoCloseable {
    void appendColumn(IReadColumn<?> sourceColumn);

    IRowAppender rowAppender(IReadColumn<?> sourceColumn);

    IReadColumn<V> read();

    void clear();
}
