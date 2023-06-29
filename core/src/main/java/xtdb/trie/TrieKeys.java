package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.FixedWidthVector;
import xtdb.vector.IRelationWriter;

import static xtdb.trie.HashTrie.LEVEL_BITS;
import static xtdb.trie.HashTrie.LEVEL_MASK;

public class TrieKeys {

    private final ArrowBufPointer leftPtr = new ArrowBufPointer();
    private final ArrowBufPointer rightPtr = new ArrowBufPointer();
    private final FixedWidthVector iidVec;

    public TrieKeys(IRelationWriter rel) {
        // TODO extend to multiple key columns
        iidVec = ((FixedWidthVector) rel.writerForName("xt$iid").getVector());
    }

    private static int bucketFor(ArrowBufPointer ptr, int level) {
        int levelOffsetBits = LEVEL_BITS * (level + 1);
        byte b = ptr.getBuf().getByte(ptr.getOffset() + (levelOffsetBits / Byte.SIZE));
        return (b >>> (levelOffsetBits % Byte.SIZE)) & LEVEL_MASK;
    }

    public int bucketFor(int idx, int level) {
        return bucketFor(iidVec.getDataPointer(idx, leftPtr), level);
    }

    public int compare(int leftIdx, int rightIdx) {
        return iidVec.getDataPointer(leftIdx, leftPtr)
                .compareTo(iidVec.getDataPointer(rightIdx, rightPtr));
    }
}
