package core2.relation;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

public interface IReadColumn<V extends ValueVector> extends AutoCloseable {

    V getVector();

    int getIndex(int idx);

    String getName();

    int getValueCount();

    IReadColumn<V> withName(String colName);

    IReadColumn<V> copy(BufferAllocator allocator);

    IReadColumn<V> select(int[] idxs);

    @Override
    default void close() {
        getVector().close();
    }
}
