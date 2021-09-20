package core2.relation;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

public interface IColumnReader<V extends ValueVector> extends AutoCloseable {

    V getVector();

    int getIndex(int idx);

    String getName();

    int getValueCount();

    IColumnReader<V> withName(String colName);

    IColumnReader<V> copy(BufferAllocator allocator);

    IColumnReader<V> select(int[] idxs);

    @Override
    default void close() {
        getVector().close();
    }
}
