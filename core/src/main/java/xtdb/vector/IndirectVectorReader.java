package xtdb.vector;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import xtdb.api.query.IKeyFn;
import xtdb.arrow.RowCopier;
import xtdb.arrow.ValueReader;
import xtdb.arrow.VectorIndirection;
import xtdb.arrow.VectorPosition;
import xtdb.util.Hasher;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static xtdb.arrow.VectorIndirection.selection;

class IndirectVectorReader implements IVectorReader {

    private static final IFn VEC_TO_WRITER = Clojure.var("xtdb.vector.writer", "->writer");

    private final IVectorReader reader;
    private final VectorIndirection indirection;

    IndirectVectorReader(IVectorReader reader, VectorIndirection indirection) {
        if (reader instanceof IndirectVectorReader ivr) {
            this.reader = ivr.reader;
            var idxs = IntStream.range(0, indirection.valueCount())
                    .map(i -> {
                        int innerIdx = indirection.getIndex(i);
                        return innerIdx < 0 ? innerIdx : ivr.indirection.getIndex(innerIdx);
                    })
                    .toArray();
            this.indirection = selection(idxs);
        } else {
            this.reader = reader;
            this.indirection = indirection;
        }
    }

    @Override
    public int getValueCount() {
        return indirection.valueCount();
    }

    @Override
    public String getName() {
        return reader.getName();
    }

    @Override
    public Field getField() {
        return reader.getField();
    }

    @Override
    public int hashCode(int idx, Hasher hasher) {
        return reader.hashCode(indirection.getIndex(idx), hasher);
    }

    @Override
    public boolean isNull(int idx) {
        int innerIdx = indirection.getIndex(idx);
        return innerIdx < 0 || reader.isNull(innerIdx);
    }

    @Override
    public boolean getBoolean(int idx) {
        return reader.getBoolean(indirection.getIndex(idx));
    }

    @Override
    public byte getByte(int idx) {
        return reader.getByte(indirection.getIndex(idx));
    }

    @Override
    public short getShort(int idx) {
        return reader.getShort(indirection.getIndex(idx));
    }

    @Override
    public int getInt(int idx) {
        return reader.getInt(indirection.getIndex(idx));
    }

    @Override
    public long getLong(int idx) {
        return reader.getLong(indirection.getIndex(idx));
    }

    @Override
    public float getFloat(int idx) {
        return reader.getFloat(indirection.getIndex(idx));
    }

    @Override
    public double getDouble(int idx) {
        return reader.getDouble(indirection.getIndex(idx));
    }

    @Override
    public ByteBuffer getBytes(int idx) {
        return reader.getBytes(indirection.getIndex(idx));
    }

    @Override
    public ArrowBufPointer getPointer(int idx, ArrowBufPointer reuse) {
        return reader.getPointer(indirection.getIndex(idx), reuse);
    }

    @Override
    public Object getObject(int idx) {
        return getObject(idx, (k) -> k);
    }

    @Override
    public Object getObject(int idx, IKeyFn<?> keyFn) {
        return reader.getObject(indirection.getIndex(idx), keyFn);
    }

    @Override
    public IVectorReader structKeyReader(String colName) {
        IVectorReader inner = reader.structKeyReader(colName);
        return inner == null ? null : new IndirectVectorReader(inner, indirection);
    }

    @Override
    public Collection<String> structKeys() {
        return reader.structKeys();
    }

    @Override
    public IVectorReader getListElements() {
        return reader.getListElements();
    }

    @Override
    public int getListStartIndex(int idx) {
        return reader.getListStartIndex(indirection.getIndex(idx));
    }

    @Override
    public int getListCount(int idx) {
        return reader.getListCount(indirection.getIndex(idx));
    }

    @Override
    public IVectorReader getMapKeys() {
        return reader.getMapKeys();
    }

    @Override
    public IVectorReader getMapValues() {
        return reader.getMapValues();
    }

    @Override
    public String getLeg(int idx) {
        return reader.getLeg(indirection.getIndex(idx));
    }

    @Override
    public List<String> legs() {
        return reader.legs();
    }

    @Override
    public IVectorReader legReader(String legKey) {
        IVectorReader inner = reader.legReader(legKey);
        return inner == null ? null : new IndirectVectorReader(inner, indirection);
    }

    @Override
    public IVectorReader copyTo(ValueVector vector) {
        IVectorWriter writer = (IVectorWriter) VEC_TO_WRITER.invoke(vector);
        var copier = rowCopier(writer);

        for (int i = 0; i < getValueCount(); i++) {
            copier.copyRow(i);
        }

        writer.syncValueCount();

        return ValueVectorReader.from(vector);
    }

    @Override
    public RowCopier rowCopier(IVectorWriter writer) {
        var inner = reader.rowCopier(writer);
        return sourceIdx -> inner.copyRow(indirection.getIndex(sourceIdx));
    }

    @Override
    public IVectorReader select(int[] idxs) {
        var sel = selection(Arrays.stream(idxs).map(indirection::getIndex).toArray());
        return new IndirectVectorReader(reader, sel);
    }

    @Override
    public IVectorReader select(int startIdx, int len) {
        return select(IntStream.range(startIdx, startIdx + len).toArray());
    }

    @Override
    public ValueReader valueReader(VectorPosition pos) {
        return reader.valueReader(new VectorPosition() {
            @Override
            public int getPosition() {
                return indirection.getIndex(pos.getPosition());
            }

            @Override
            public void setPosition(int position) {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Override
    public String toString() {
        return "(IndirectVectorReader {reader=%s, indirection=%s})".formatted(reader, indirection);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

}
