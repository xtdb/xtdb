package xtdb.trie;

public interface HashTrie {

    int LEVEL_BITS = 4;
    int LEVEL_WIDTH = 1 << LEVEL_BITS;
    int LEVEL_MASK = LEVEL_WIDTH - 1;

    interface Visitor<R> {
        default R visitBranch(HashTrie[] children) {
            for (HashTrie child : children) {
                child.accept(this);
            }

            return null;
        }

        R visitLeaf(int pageIdx, int[] selection);
    }

    <R> R accept(Visitor<R> visitor);
}

