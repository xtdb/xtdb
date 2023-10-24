package xtdb.vector;

import clojure.lang.Keyword;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

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

    IVectorReader structKeyReader(String colName);

    Collection<String> structKeys();

    IVectorReader listElementReader();

    int getListStartIndex(int idx);

    int getListCount(int idx);

    Keyword getLeg(int idx);

    IVectorReader legReader(Keyword legKey);

    List<Keyword> legs();

    default IVectorReader copy(BufferAllocator allocator) {
        return copyTo(getField().createVector(allocator)).withName(getName());
    }

    IVectorReader copyTo(ValueVector vector);

    IVectorReader transferTo(ValueVector vector);

    default IVectorReader select(int[] idxs) {
        return new IndirectVectorReader(this, new IVectorIndirection.Selection(idxs));
    }

    default IVectorReader select(int startIdx, int len) {
        return new IndirectVectorReader(this, new IVectorIndirection.Slice(startIdx, len));
    }

    IRowCopier rowCopier(IVectorWriter writer);

    // TODO temporary, while we port the expression engine over
    IMonoVectorReader monoReader(Object colType);
    IPolyVectorReader polyReader(Object colType);
}
