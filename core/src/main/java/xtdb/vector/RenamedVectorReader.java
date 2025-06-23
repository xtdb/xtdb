package xtdb.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xtdb.api.query.IKeyFn;
import xtdb.arrow.*;
import xtdb.util.Hasher;

import java.nio.ByteBuffer;
import java.util.Set;

public class RenamedVectorReader implements IVectorReader {

    private final VectorReader reader;
    private final String colName;

    public RenamedVectorReader(VectorReader reader, String colName) {
        this.reader = reader;
        this.colName = colName;
    }

    @Override
    public int getValueCount() {
        return reader.getValueCount();
    }

    @Override
    public String getName() {
        return colName;
    }

    @Override
    public VectorReader withName(String colName) {
        return new RenamedVectorReader(reader, colName);
    }

    @Override
    public Field getField() {
        var field = reader.getField();
        return new Field(colName, field.getFieldType(), field.getChildren());
    }

    @Override
    public int hashCode(int idx, Hasher hasher) {
        return reader.hashCode(idx, hasher);
    }

    @Override
    public boolean isNull(int idx) {
        return reader.isNull(idx);
    }

    @Override
    public boolean getBoolean(int idx) {
        return reader.getBoolean(idx);
    }

    @Override
    public byte getByte(int idx) {
        return reader.getByte(idx);
    }

    @Override
    public short getShort(int idx) {
        return reader.getShort(idx);
    }

    @Override
    public int getInt(int idx) {
        return reader.getInt(idx);
    }

    @Override
    public long getLong(int idx) {
        return reader.getLong(idx);
    }

    @Override
    public float getFloat(int idx) {
        return reader.getFloat(idx);
    }

    @Override
    public double getDouble(int idx) {
        return reader.getDouble(idx);
    }

    @Override
    public ByteBuffer getBytes(int idx) {
        return reader.getBytes(idx);
    }

    @Override
    public ArrowBufPointer getPointer(int idx, ArrowBufPointer reuse) {
        return reader.getPointer(idx, reuse);
    }

    @Override
    public Object getObject(int idx) {
        return reader.getObject(idx, (k) -> k);
    }
    @Override
    public Object getObject(int idx, IKeyFn<?> keyFn) {
        return reader.getObject(idx, keyFn);
    }

    @Override
    public @Nullable VectorReader vectorForOrNull(@NotNull String name) {
        return reader.vectorForOrNull(name);
    }

    @Override
    public Set<String> getKeyNames() {
        return reader.getKeyNames();
    }

    @Override
    public VectorReader getListElements() {
        return reader.getListElements();
    }

    @Override
    public int getListStartIndex(int idx) {
        return reader.getListStartIndex(idx);
    }

    @Override
    public int getListCount(int idx) {
        return reader.getListCount(idx);
    }

    @Override
    public VectorReader getMapKeys() {
        return reader.getMapKeys();
    }

    @Override
    public VectorReader getMapValues() {
        return reader.getMapValues();
    }

    @Override
    public String getLeg(int idx) {
        return reader.getLeg(idx);
    }

    @Override
    public Set<String> getLegNames() {
        return reader.getLegNames();
    }

    @Override
    public VectorReader openSlice(BufferAllocator allocator) {
        return new RenamedVectorReader(reader.openSlice(allocator), colName);
    }

    @Override
    public VectorReader copyTo(ValueVector vector) {
        return new RenamedVectorReader(((IVectorReader) reader).copyTo(vector), colName);
    }

    @Override
    public VectorReader select(int[] idxs) {
        return new RenamedVectorReader(reader.select(idxs), colName);
    }

    @Override
    public VectorReader select(int startIdx, int len) {
        return new RenamedVectorReader(reader.select(startIdx, len), colName);
    }

    @Override
    public RowCopier rowCopier(VectorWriter writer) {
        return reader.rowCopier(writer);
    }

    @Override
    public ValueReader valueReader(VectorPosition pos) {
        return reader.valueReader(pos);
    }

    @Override
    public String toString() {
        return "(RenamedVectorReader {colName='%s', reader=%s})".formatted(colName, reader);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
