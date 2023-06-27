package xtdb.trie;

import java.util.stream.IntStream;

public interface HashTrie {

    int LEVEL_BITS = 4;
    int LEVEL_WIDTH = 1 << LEVEL_BITS;
    int LEVEL_MASK = LEVEL_WIDTH - 1;

    HashTrie add(int idx);

    interface Leaf {
        int size();

        int get(int idx);

        default IntStream indices() {
            return IntStream.range(0, size()).map(this::get);
        }
    }

    interface Visitor<R> {
        default R visitBranch(HashTrie[] children) {
            for (HashTrie child : children) {
                child.accept(this);
            }

            return null;
        }

        R visitLeaf(Leaf leaf);
    }

    <R> R accept(Visitor<R> visitor);
}

