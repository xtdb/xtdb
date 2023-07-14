package xtdb.vector;

import java.util.Arrays;

public interface IVectorIndirection {
    int valueCount();

    int getIndex(int idx);

    record Selection(int[] idxs) implements IVectorIndirection {

        @Override
        public int valueCount() {
            return idxs.length;
        }

        @Override
        public int getIndex(int idx) {
            return idxs[idx];
        }

        @Override
        public String toString() {
            return "(Selection {idxs=%s})".formatted(Arrays.toString(idxs));
        }
    }

    record Slice(int startIdx, int len) implements IVectorIndirection {
        @Override
        public int valueCount() {
            return len;
        }

        @Override
        public int getIndex(int idx) {
            return startIdx + idx;
        }

    }
}
