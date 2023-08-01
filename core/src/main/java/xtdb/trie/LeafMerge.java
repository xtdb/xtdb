package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class LeafMerge {

    private LeafMerge() {
    }

    public interface LeafPointer {
        int getLeafOrdinal();

        ArrowBufPointer getPointer(ArrowBufPointer reuse);

        /**
         * @return true to continue, false to terminate
         */
        boolean pick();

        boolean isValid();
    }

    private static Comparator<LeafPointer> leafComparator() {
        var leftBufPtr = new ArrowBufPointer();
        var rightBufPtr = new ArrowBufPointer();

        return ((Comparator<LeafPointer>) (l, r) -> l.getPointer(leftBufPtr).compareTo(r.getPointer(rightBufPtr)))
                .thenComparing((l, r) -> Long.compare(r.getLeafOrdinal(), l.getLeafOrdinal()));
    }

    /**
     * @param leaves ordered leaves to merge. assume leaves are sorted internally by sys-time desc,
     *               and that every entry in leaf[n+1] is later than every entry in leaf[n],
     *               or that the ordering doesn't matter to the result (e.g. independent tries at the same chunk - T1 and T2)
     */
    public static void merge(List<LeafPointer> leaves) {
        var pq = new PriorityQueue<>(leafComparator());

        for (LeafPointer leaf : leaves) {
            if (leaf.isValid()) {
                pq.add(leaf);
            }
        }

        // keep taking from the PQ until we hit the end of the path
        while (true) {
            var leaf = pq.poll();
            if (leaf == null) return;

            if (!leaf.pick()) return;

            if (leaf.isValid()) {
                pq.add(leaf);
            }
        }
    }
}
