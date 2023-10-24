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

public class RenamedVectorReader implements IVectorReader {

    private final IVectorReader reader;
    private final String colName;

    public RenamedVectorReader(IVectorReader reader, String colName) {
        this.reader = reader;
        this.colName = colName;
    }

    @Override
    public int valueCount() {
        return reader.valueCount();
    }

    @Override
    public String getName() {
        return colName;
    }

    @Override
    public IVectorReader withName(String colName) {
        return new RenamedVectorReader(reader, colName);
    }

    @Override
    public Field getField() {
        var field = reader.getField();
        return new Field(colName, field.getFieldType(), field.getChildren());
    }

    @Override
    public int hashCode(int idx, ArrowBufHasher hasher) {
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
    public ArrowBufPointer getPointer(int idx) {
        return reader.getPointer(idx);
    }

    @Override
    public ArrowBufPointer getPointer(int idx, ArrowBufPointer reuse) {
        return reader.getPointer(idx, reuse);
    }

    @Override
    public Object getObject(int idx) {
        return reader.getObject(idx);
    }

    @Override
    public IVectorReader structKeyReader(String colName) {
        return reader.structKeyReader(colName);
    }

    @Override
    public Collection<String> structKeys() {
        return reader.structKeys();
    }

    @Override
    public IVectorReader listElementReader() {
        return reader.listElementReader();
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
    public Keyword getLeg(int idx) {
        return reader.getLeg(idx);
    }

    @Override
    public IVectorReader legReader(Keyword legKey) {
        return reader.legReader(legKey);
    }

    @Override
    public List<Keyword> legs() {
        return reader.legs();
    }

    @Override
    public IVectorReader copy(BufferAllocator allocator) {
        return new RenamedVectorReader(reader.copy(allocator), colName);
    }

    @Override
    public IVectorReader copyTo(ValueVector vector) {
        return new RenamedVectorReader(reader.copyTo(vector), colName);
    }

    @Override
    public IVectorReader transferTo(ValueVector vector) {
        return new RenamedVectorReader(reader.transferTo(vector), colName);
    }

    @Override
    public IVectorReader select(int[] idxs) {
        return new RenamedVectorReader(reader.select(idxs), colName);
    }

    @Override
    public IVectorReader select(int startIdx, int len) {
        return new RenamedVectorReader(reader.select(startIdx, len), colName);
    }

    @Override
    public IRowCopier rowCopier(IVectorWriter writer) {
        return reader.rowCopier(writer);
    }

    @Override
    public IMonoVectorReader monoReader(Object colType) {
        return reader.monoReader(colType);
    }

    @Override
    public IPolyVectorReader polyReader(Object colType) {
        return reader.polyReader(colType);
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
