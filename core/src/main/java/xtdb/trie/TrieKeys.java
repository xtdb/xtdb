package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Arrays;

public class TrieKeys {

    public static final int LEVEL_BITS = 4;
    public static final int LEVEL_WIDTH = 1 << LEVEL_BITS;
    public static final int LEVEL_MASK = LEVEL_WIDTH - 1;

    private final BaseFixedWidthVector[] keyVectors;

    private final ArrowBufPointer bucketPtr = new ArrowBufPointer();
    private final ArrowBufPointer leftPtr = new ArrowBufPointer();
    private final ArrowBufPointer rightPtr = new ArrowBufPointer();

    public TrieKeys(BaseFixedWidthVector[] keyVectors) {
        this.keyVectors = keyVectors;
    }

    public int bucketFor(int idx, int level) {
        // TODO unit test for multiple key vecs
        for (BaseFixedWidthVector keyVector : keyVectors) {
            int levelOffsetBits = LEVEL_BITS * (level + 1);
            int levelOffsetBytes = levelOffsetBits / Byte.SIZE;
            int typeWidth = keyVector.getTypeWidth();
            if (levelOffsetBytes < typeWidth) {
                keyVector.getDataPointer(idx, bucketPtr);

                byte b = bucketPtr.getBuf().getByte(bucketPtr.getOffset() + levelOffsetBytes);
                return (b >>> (levelOffsetBits % Byte.SIZE)) & LEVEL_MASK;
            } else {
                level -= (typeWidth * Byte.SIZE) / LEVEL_BITS;
            }
        }

        throw new IllegalStateException();
    }

    public int compare(int leftIdx, int rightIdx) {
        // TODO unit test for multiple key vecs
        for (BaseFixedWidthVector keyVector : keyVectors) {
            int cmp = keyVector.getDataPointer(leftIdx, leftPtr).compareTo(keyVector.getDataPointer(rightIdx, rightPtr));
            if (cmp != 0) return cmp;
        }

        return 0;
    }

    public TrieKeys withLeaf(VectorSchemaRoot leaf) {
        BaseFixedWidthVector[] newKeyVectors =
                Arrays.stream(keyVectors)
                        .map(vec -> ((BaseFixedWidthVector) leaf.getVector(vec.getField())))
                        .toArray(BaseFixedWidthVector[]::new);
        return new TrieKeys(newKeyVectors);
    }
}
