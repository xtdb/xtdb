package xtdb.vector;

import clojure.lang.PersistentVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import xtdb.api.query.IKeyFn;
import xtdb.arrow.RowCopier;
import xtdb.arrow.ValueReader;
import xtdb.arrow.VectorPosition;
import xtdb.util.Hasher;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static xtdb.arrow.VectorIndirection.selection;
import static xtdb.arrow.VectorIndirection.slice;

public interface IVectorReader extends AutoCloseable {

    int valueCount();

    String getName();

    default IVectorReader withName(String colName) {
        return new RenamedVectorReader(this, colName);
    }

    Field getField();

    int hashCode(int idx, Hasher hasher);

    private static RuntimeException unsupported(IVectorReader rdr) {
        throw new UnsupportedOperationException(rdr.getClass().getName());
    }

    default boolean isNull(int idx) {
        throw unsupported(this);
    }

    default boolean getBoolean(int idx) {
        throw unsupported(this);
    }

    default byte getByte(int idx) {
        throw unsupported(this);
    }

    default short getShort(int idx) {
        throw unsupported(this);
    }

    default int getInt(int idx) {
        throw unsupported(this);
    }

    default long getLong(int idx) {
        throw unsupported(this);
    }

    default float getFloat(int idx) {
        throw unsupported(this);
    }

    default double getDouble(int idx) {
        throw unsupported(this);
    }

    default ByteBuffer getBytes(int idx) {
        throw unsupported(this);
    }

    default ArrowBufPointer getPointer(int idx) {
        throw unsupported(this);
    }

    default ArrowBufPointer getPointer(int idx, ArrowBufPointer reuse) {
        throw unsupported(this);
    }

    Object getObject(int idx);

    Object getObject(int idx, IKeyFn<?> keyFn);

    default List<?> toList() {
        return PersistentVector.create(IntStream.range(0, valueCount()).mapToObj(this::getObject).toList());
    }

    default List<Object> toList(IKeyFn<?> keyFn) {
        return IntStream.range(0, valueCount()).mapToObj(idx -> getObject(idx, keyFn)).toList();
    }

    default IVectorReader structKeyReader(String colName) {
        throw unsupported(this);
    }

    default Collection<String> structKeys() {
        throw unsupported(this);
    }

    default IVectorReader listElementReader() {
        throw unsupported(this);
    }

    default int getListStartIndex(int idx) {
        throw unsupported(this);
    }

    default int getListCount(int idx) {
        throw unsupported(this);
    }

    default IVectorReader mapKeyReader() {
        throw unsupported(this);
    }

    default IVectorReader mapValueReader() {
        throw unsupported(this);
    }

    default String getLeg(int idx) {
        throw unsupported(this);
    }

    default IVectorReader legReader(String legKey) {
        throw unsupported(this);
    }

    default List<String> legs() {
        throw unsupported(this);
    }

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
