package xtdb.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import xtdb.api.query.IKeyFn;
import xtdb.arrow.RowCopier;
import xtdb.arrow.ValueReader;
import xtdb.arrow.VectorPosition;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import static xtdb.arrow.VectorIndirection.selection;
import static xtdb.arrow.VectorIndirection.slice;

public interface IVectorReader extends AutoCloseable {

    int valueCount();

    String getName();

    IVectorReader withName(String colName);

    Field getField();

    int hashCode(int idx, ArrowBufHasher hasher);

    boolean isNull(int idx);

    boolean getBoolean(int idx);

    byte getByte(int idx);

    short getShort(int idx);

    int getInt(int idx);

    long getLong(int idx);

    float getFloat(int idx);

    double getDouble(int idx);

    ByteBuffer getBytes(int idx);

    ArrowBufPointer getPointer(int idx);

    ArrowBufPointer getPointer(int idx, ArrowBufPointer reuse);

    Object getObject(int idx);

    Object getObject(int idx, IKeyFn<?> keyFn);

    IVectorReader structKeyReader(String colName);

    Collection<String> structKeys();

    IVectorReader listElementReader();

    int getListStartIndex(int idx);

    int getListCount(int idx);

    IVectorReader mapKeyReader();

    IVectorReader mapValueReader();

    String getLeg(int idx);

    IVectorReader legReader(String legKey);

    List<String> legs();

    default IVectorReader copy(BufferAllocator allocator) {
        return copyTo(getField().createVector(allocator)).withName(getName());
    }

    IVectorReader copyTo(ValueVector vector);

    default IVectorReader select(int[] idxs) {
        return new IndirectVectorReader(this, selection(idxs));
    }

    default IVectorReader select(int startIdx, int len) {
        return new IndirectVectorReader(this, slice(startIdx, len));
    }

    RowCopier rowCopier(IVectorWriter writer);

    ValueReader valueReader(VectorPosition pos);

    @Override
    void close();
}
