package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;
import xtdb.vector.IVectorReader;

import java.util.Collection;
import java.util.Objects;
import java.util.PriorityQueue;

public class LeafMergeQueue {
    public static final class LeafPointer {
        private final int ordinal;
        private int index;

        public LeafPointer(int ordinal) {
            this.ordinal = ordinal;
        }

        public int getOrdinal() {
            return ordinal;
        }

        public int getIndex() {
            return index;
        }

        public void reset() {
            index = 0;
        }

        @Override
        public String toString() {
            return "(LeafPointer {:ordinal %d, :index %d})".formatted(ordinal, index);
        }
    }

    private final ArrowBufPointer leftCmp = new ArrowBufPointer();
    private final ArrowBufPointer rightCmp = new ArrowBufPointer();
    private final ArrowBufPointer isValidCmp = new ArrowBufPointer();

    private final byte[] path;
    private final IVectorReader[] rdrs;
    private final PriorityQueue<LeafPointer> pq = new PriorityQueue<>((l, r) -> {
        int cmp = getPointer(l, leftCmp).compareTo(getPointer(r, rightCmp));
        if (cmp != 0) return cmp;
        return Long.compare(r.ordinal, l.ordinal);
    });

    public LeafMergeQueue(byte[] path, IVectorReader[] rdrs, Collection<LeafPointer> lps) {
        this.rdrs = rdrs;
        this.path = path;
        pq.addAll(lps.stream().filter(Objects::nonNull).filter(this::isValid).toList());
    }

    public LeafMergeQueue(byte[] path, IVectorReader[] rdrs) {
        this.rdrs = rdrs;
        this.path = path;
        for (int i = 0; i < rdrs.length; i++) {
            var rdr = rdrs[i];
            if (rdr != null) {
                var lp = new LeafPointer(i);
                if (isValid(lp)) {
                    pq.add(lp);
                }
            }
        }
    }

    private ArrowBufPointer getPointer(LeafPointer lp, ArrowBufPointer ptr) {
        return rdrs[lp.ordinal].getPointer(lp.index, ptr);
    }

    private boolean isValid(LeafPointer lp) {
        return lp.index < rdrs[lp.ordinal].valueCount() && HashTrie.compareToPath(getPointer(lp, isValidCmp), path) <= 0;
    }

    public void advance(LeafPointer lp) {
        lp.index++;
        if (isValid(lp)) {
            pq.add(lp);
        }
    }

    public LeafPointer poll() {
        return pq.poll();
    }
}
