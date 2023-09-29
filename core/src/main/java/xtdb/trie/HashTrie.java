package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public interface HashTrie<N extends HashTrie.Node<N>> {

    int LEVEL_BITS = 2;
    int LEVEL_WIDTH = 1 << LEVEL_BITS;
    int LEVEL_MASK = LEVEL_WIDTH - 1;

    Node<N> rootNode();

    default List<? extends Node<N>> leaves() {
        return rootNode().leaves();
    }

    interface Node<N extends Node<N>> {
        byte[] path();

        N[] children();

        default Stream<? extends HashTrie.Node<N>> leafStream() {
            var children = children();
            return children == null
                    ? Stream.of(this)
                    : Arrays.stream(children).flatMap(child -> child == null ? null : child.leafStream());
        }

        default List<? extends Node<N>> leaves() {
            return leafStream().toList();
        }
    }

    static byte bucketFor(ArrowBufPointer pointer, int level) {
        int bitIdx = level * LEVEL_BITS;
        int byteIdx = bitIdx / Byte.SIZE;
        int bitOffset = bitIdx % Byte.SIZE;

        byte b = pointer.getBuf().getByte(pointer.getOffset() + byteIdx);
        return (byte) ((b >>> ((Byte.SIZE - LEVEL_BITS) - bitOffset)) & LEVEL_MASK);
    }

    static int compareToPath(ArrowBufPointer pointer, byte[] path) {
        for (int level = 0; level < path.length; level++) {
            var cmp = Integer.compare(bucketFor(pointer, level), path[level]);
            if (cmp != 0) return cmp;
        }

        return 0;
    }
}
