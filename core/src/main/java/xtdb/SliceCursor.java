package xtdb;

import xtdb.vector.RelationReader;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

public class SliceCursor implements ICursor<RelationReader> {
    private final RelationReader rel;
    private int startIdx;
    private final OfInt rowCounts;

    public SliceCursor(RelationReader rel, int[] rowCounts) {
        this.rel = rel;
        this.rowCounts = Arrays.stream(rowCounts).spliterator();
    }

    @Override
    public boolean tryAdvance(Consumer<? super RelationReader> c) {
        return rowCounts.tryAdvance((IntConsumer) rowCount -> {
            c.accept(rel.select(startIdx, rowCount));
            SliceCursor.this.startIdx += rowCount;
        });
    }
}
