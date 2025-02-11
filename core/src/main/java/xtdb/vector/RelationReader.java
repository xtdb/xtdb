package xtdb.vector;

import org.apache.arrow.memory.BufferAllocator;
import xtdb.api.query.IKeyFn;
import xtdb.arrow.Relation;
import xtdb.arrow.Vector;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RelationReader implements Iterable<IVectorReader>, AutoCloseable {

    private final Map<String, IVectorReader> cols;
    private final int rowCount;

    public static RelationReader from(List<IVectorReader> cols) {
        return from(cols, cols.stream().mapToInt(IVectorReader::valueCount).findFirst().orElse(0));
    }

    public static RelationReader from(List<IVectorReader> cols, int rowCount) {
        var colsMap = new LinkedHashMap<String, IVectorReader>();

        for (IVectorReader col : cols) {
            colsMap.put(col.getName(), col);
        }

        return new RelationReader(colsMap, rowCount);
    }

    private RelationReader(Map<String, IVectorReader> cols, int rowCount) {
        this.cols = cols;
        this.rowCount = rowCount;
    }

    public int rowCount() {
        return rowCount;
    }

    public <K> Map<K, ?> getRow(int idx, IKeyFn<K> keyFn) {
        return cols.values().stream()
                .collect(Collectors.toMap(
                        e -> keyFn.denormalize(e.getName()),
                        e -> e.getObject(idx)));
    }

    public Map<String, ?> getRow(int idx) {
        return getRow(idx, k -> k);
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

    public Relation openAsRelation(BufferAllocator allocator) {
        return new Relation(cols.values().stream()
                .map(vr -> {
                    var outVec = vr.getField().createVector(allocator);
                    vr.copyTo(outVec);
                    var res = Vector.fromArrow(outVec);
                    outVec.close();
                    return res;
                })
                .toList());
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
