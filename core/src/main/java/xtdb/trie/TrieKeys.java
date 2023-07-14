package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.FixedSizeBinaryVector;

public class TrieKeys {

    public static final int LEVEL_BITS = 4;
    public static final int LEVEL_WIDTH = 1 << LEVEL_BITS;
    public static final int LEVEL_MASK = LEVEL_WIDTH - 1;

    private final FixedSizeBinaryVector iidVector;

    private final ArrowBufPointer bucketPtr = new ArrowBufPointer();
    private final ArrowBufPointer leftPtr = new ArrowBufPointer();
    private final ArrowBufPointer rightPtr = new ArrowBufPointer();

    public TrieKeys(FixedSizeBinaryVector iidVector) {
        this.iidVector = iidVector;
    }

    public int bucketFor(int idx, int level) {
        int levelOffsetBits = LEVEL_BITS * (level + 1);
        int levelOffsetBytes = (levelOffsetBits - LEVEL_BITS) / Byte.SIZE;

        iidVector.getDataPointer(idx, bucketPtr);

        byte b = bucketPtr.getBuf().getByte(bucketPtr.getOffset() + levelOffsetBytes);
        return (b >>> (levelOffsetBits % Byte.SIZE)) & LEVEL_MASK;
    }

    public int compare(int leftIdx, int rightIdx) {
        int cmp = iidVector.getDataPointer(leftIdx, leftPtr).compareTo(iidVector.getDataPointer(rightIdx, rightPtr));
        if (cmp != 0) return cmp;

        // sort by idx desc
        return Long.compare(rightIdx, leftIdx);
    }
}
