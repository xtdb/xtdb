package core2.vector;

import core2.vector.reader.IMonoVectorReader;
import core2.vector.reader.IPolyVectorReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Set;

public interface IIndirectVector<V extends ValueVector> extends AutoCloseable {

    boolean isPresent(int idx);

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

    IRowCopier rowCopier(IVectorWriter<? super V> writer);

    IStructReader structReader();
    IListReader listReader();

    @Override
    default void close() {
        getVector().close();
    }

    IMonoVectorReader monoReader();
    IPolyVectorReader polyReader(List<Object> orderedColTypes);
}
