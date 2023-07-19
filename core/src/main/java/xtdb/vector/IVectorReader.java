package xtdb.vector;

import clojure.lang.Keyword;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.ByteBuffer;
import java.util.Collection;

public interface IVectorReader extends AutoCloseable {

    int valueCount();

    String getName();

    IVectorReader withName(String colName);

    Field getField();

    int hashCode(int idx, ArrowBufHasher hasher);

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

    int getListStartIndex(int idx);

    int getListCount(int idx);

    byte getTypeId(int idx);

    Keyword getLeg(int idx);

    IVectorReader typeIdReader(byte typeId);

    IVectorReader legReader(Keyword legKey);

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

    Collection<? extends IVectorReader> metadataReaders();

    IVectorReader listElementReader();

    Collection<Keyword> legs();
}
