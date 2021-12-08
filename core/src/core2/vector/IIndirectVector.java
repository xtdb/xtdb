package core2.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

public interface IIndirectVector<V extends ValueVector> extends AutoCloseable {

    V getVector();

    int getIndex(int idx);

    String getName();

    int getValueCount();

    IIndirectVector<V> withName(String colName);

    @SuppressWarnings("unchecked")
    default IIndirectVector<V> copy(BufferAllocator allocator) {
        return copyTo((V) getVector().getField().createVector(allocator));
    }

    IIndirectVector<V> copyTo(V vector);

    IIndirectVector<V> select(int[] idxs);

    @Override
    default void close() {
        getVector().close();
    }
}
