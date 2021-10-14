package core2.vector;

import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IIndirectRelation extends AutoCloseable, Iterable<IIndirectVector<?>> {

    <V extends ValueVector> IIndirectVector<V> vectorForName(String colName);

    int rowCount();

    @Override
    default void close() throws Exception {
    }
}
