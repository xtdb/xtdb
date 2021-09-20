package core2.relation;

import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IRelationReader extends AutoCloseable, Iterable<IColumnReader<?>> {

    <V extends ValueVector> IColumnReader<V> columnReader(String colName);

    int rowCount();

    @Override
    default void close() throws Exception {
    }
}
