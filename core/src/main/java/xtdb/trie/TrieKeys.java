package xtdb.trie;

public interface TrieKeys {
    int groupFor(int idx, int level);

    int compare(int leftIdx, int rightIdx);
}
