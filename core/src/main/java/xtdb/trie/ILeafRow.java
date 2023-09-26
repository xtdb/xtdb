package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;

import java.util.Comparator;

public interface ILeafRow {
    ArrowBufPointer getIidPointer(ArrowBufPointer reuse);

    long getSystemTime();

    static Comparator<? extends ILeafRow> comparator() {
        var leftCmp = new ArrowBufPointer();
        var rightCmp = new ArrowBufPointer();

        return (l, r) -> {
            int cmp = l.getIidPointer(leftCmp).compareTo(r.getIidPointer(rightCmp));
            if (cmp != 0) return cmp;
            return Long.compare(r.getSystemTime(), l.getSystemTime());
        };
    }

    int getIndex();
    int nextIndex();

    int rowCount();

    default boolean isValid(ArrowBufPointer reuse, byte[] path) {
        return getIndex() < rowCount() && HashTrie.compareToPath(getIidPointer(reuse), path) <= 0;
    }
}
