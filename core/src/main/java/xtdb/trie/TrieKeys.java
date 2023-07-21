package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.ElementAddressableVector;

public class TrieKeys {

    public static final int LEVEL_BITS = 4;
    public static final int LEVEL_WIDTH = 1 << LEVEL_BITS;
    public static final int LEVEL_MASK = LEVEL_WIDTH - 1;

    private final ElementAddressableVector iidVector;

    private final ArrowBufPointer bucketPtr = new ArrowBufPointer();
    private final ArrowBufPointer leftPtr = new ArrowBufPointer();
    private final ArrowBufPointer rightPtr = new ArrowBufPointer();

    public TrieKeys(ElementAddressableVector iidVector) {
        this.iidVector = iidVector;
    }

    public static byte bucketFor(ArrowBufPointer pointer, int level) {
        int levelOffsetBits = LEVEL_BITS * (level + 1);
        int levelOffsetBytes = (levelOffsetBits - LEVEL_BITS) / Byte.SIZE;

        byte b = pointer.getBuf().getByte(pointer.getOffset() + levelOffsetBytes);
        return (byte) ((b >>> (levelOffsetBits % Byte.SIZE)) & LEVEL_MASK);
    }

    public byte bucketFor(int idx, int level) {
        return bucketFor(iidVector.getDataPointer(idx, bucketPtr), level);
    }

    public int compare(int leftIdx, int rightIdx) {
        int cmp = iidVector.getDataPointer(leftIdx, leftPtr).compareTo(iidVector.getDataPointer(rightIdx, rightPtr));
        if (cmp != 0) return cmp;

        // sort by idx desc
        return Integer.compare(rightIdx, leftIdx);
    }

    public static int compareToPath(ArrowBufPointer pointer, byte[] path) {
        for (int level = 0; level < path.length; level++) {
            var cmp = Integer.compare(bucketFor(pointer, level), path[level]);
            if (cmp != 0) return cmp;
        }

        return 0;
    }

    public static int comparePaths(byte[] path1, byte[] path2) {
        for (int level = 0; level < Math.min(path1.length , path2.length); level++) {
            var cmp = Integer.compare(path1[level], path2[level]);
            if (cmp != 0) return cmp;
        }
        if (path1.length < path2.length) return -1;
        if (path1.length < path2.length) return 0;
        return 0;
    }

    public int compareToPath(int idx, byte[] path) {
        return compareToPath(iidVector.getDataPointer(idx, bucketPtr), path);
    }
}
