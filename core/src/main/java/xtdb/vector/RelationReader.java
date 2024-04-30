package xtdb.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.*;
import java.util.function.Function;

public class RelationReader implements Iterable<IVectorReader>, AutoCloseable {

    private final Map<String, IVectorReader> cols;
    private final int rowCount;

    public Field getField() {
        return field;
    }

    private final Field field;

    public static RelationReader from(List<IVectorReader> cols) {
        return from(cols, cols.stream().mapToInt(IVectorReader::valueCount).findFirst().orElse(0));
    }

    public static RelationReader from(List<IVectorReader> cols, int rowCount) {
        var colsMap = new LinkedHashMap<String, IVectorReader>();

        for (IVectorReader col : cols) {
            colsMap.put(col.getName(), col);
        }

        return new RelationReader(colsMap, rowCount, new Field("rel", FieldType.notNullable(ArrowType.Struct.INSTANCE), cols.stream().map(IVectorReader::getField).toList()));
    }

    private RelationReader(Map<String, IVectorReader> cols, int rowCount, Field field) {
        this.cols = cols;
        this.rowCount = rowCount;
        this.field = field;
    }

    public int rowCount() {
        return rowCount;
    }

    public IVectorReader readerForName(String colName) {
        return cols.get(colName);
    }

    private RelationReader from(Function<IVectorReader, IVectorReader> f, int rowCount) {
        return from(cols.values().stream().map(f).toList(), rowCount);
    }

    public RelationReader select(int[] idxs) {
        return from(vr -> vr.select(idxs), idxs.length);
    }

    public RelationReader select(int startIdx, int len) {
        return from(vr -> vr.select(startIdx, len), len);
    }

    public RelationReader copy(BufferAllocator allocator) {
        return from(vr -> vr.copy(allocator), rowCount);
    }

    @Override
    public Iterator<IVectorReader> iterator() {
        return cols.values().iterator();
    }

    @Override
    public Spliterator<IVectorReader> spliterator() {
        return cols.values().spliterator();
    }

    @Override
    public String toString() {
        return "(RelationReader {rowCount=%d, cols=%s})".formatted(rowCount, cols);
    }

    @Override
    public void close() throws Exception {
        for (IVectorReader vr : cols.values()) {
            vr.close();
        }
    }
}
