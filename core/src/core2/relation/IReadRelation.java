package core2.relation;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IReadRelation extends AutoCloseable, Iterable<IReadColumn<?>> {

    <V extends ValueVector> IReadColumn<V> readColumn(String colName);

    int rowCount();

    @Override
    default void close() throws Exception {
    }
}
