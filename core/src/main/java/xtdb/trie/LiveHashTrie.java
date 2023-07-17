package xtdb.trie;

import java.util.Arrays;
import java.util.stream.IntStream;

import static xtdb.trie.TrieKeys.LEVEL_WIDTH;

public record LiveHashTrie(Node rootNode, TrieKeys trieKeys) implements HashTrie<LiveHashTrie.Node> {

    private static final int LOG_LIMIT = 64;
    private static final int PAGE_LIMIT = 1024;

    public sealed interface Node extends HashTrie.Node<Node> {
        Node add(LiveHashTrie trie, int idx);

        Node compactLogs(LiveHashTrie trie);
    }

    public static class Builder {
        private final TrieKeys trieKeys;
        private int logLimit = LOG_LIMIT;
        private int pageLimit = PAGE_LIMIT;

        private Builder(TrieKeys trieKeys) {
            this.trieKeys = trieKeys;
        }

        public void setLogLimit(int logLimit) {
            this.logLimit = logLimit;
        }

        public void setPageLimit(int pageLimit) {
            this.pageLimit = pageLimit;
        }

        public LiveHashTrie build() {
            return new LiveHashTrie(new Leaf(logLimit, pageLimit), trieKeys);
        }
    }

    public static Builder builder(TrieKeys trieKeys) {
        return new Builder(trieKeys);
    }

    @SuppressWarnings("unused")
    public static LiveHashTrie emptyTrie(TrieKeys trieKeys) {
        return new LiveHashTrie(new Leaf(LOG_LIMIT, PAGE_LIMIT), trieKeys);
    }

    public LiveHashTrie add(int idx) {
        return new LiveHashTrie(rootNode.add(this, idx), trieKeys);
    }

    public LiveHashTrie compactLogs() {
        return new LiveHashTrie(rootNode.compactLogs(this), trieKeys);
    }

    private int bucketFor(int idx, int level) {
        return trieKeys.bucketFor(idx, level);
    }

    private int compare(int leftIdx, int rightIdx) {
        return trieKeys.compare(leftIdx, rightIdx);
    }

    private static byte[] conjPath(byte[] path, byte idx) {
        int currentPathLength = path.length;
        var childPath = new byte[currentPathLength + 1];
        System.arraycopy(path, 0, childPath, 0, currentPathLength);
        childPath[currentPathLength] = idx;
        return childPath;
    }

    public record Branch(int logLimit, int pageLimit, byte[] path, Node[] children) implements Node {

        @Override
        public Node add(LiveHashTrie trie, int idx) {
            var bucket = trie.bucketFor(idx, path.length);

            var newChildren = IntStream.range(0, children.length)
                    .mapToObj(childIdx -> {
                        var child = children[childIdx];
                        if (bucket == childIdx) {
                            if (child == null) {
                                child = new Leaf(logLimit, pageLimit, conjPath(path, (byte) childIdx));
                            }
                            child = child.add(trie, idx);
                        }
                        return child;
                    }).toArray(Node[]::new);

            return new Branch(logLimit, pageLimit, path, newChildren);
        }

        @Override
        public Node compactLogs(LiveHashTrie trie) {
            var children =
                    Arrays.stream(this.children)
                            .map(child -> child == null ? null : child.compactLogs(trie))
                            .toArray(Node[]::new);

            return new Branch(logLimit, pageLimit, path, children);
        }
    }

    public record Leaf(int logLimit, int pageLimit, byte[] path, int[] data, int[] log, int logCount) implements Node {

        Leaf(int logLimit, int pageLimit) {
            this(logLimit, pageLimit, new byte[0]);
        }

        Leaf(int logLimit, int pageLimit, byte[] path) {
            this(logLimit, pageLimit, path, new int[0]);
        }

        private Leaf(int logLimit, int pageLimit, byte[] path, int[] data) {
            this(logLimit, pageLimit, path, data, new int[logLimit], 0);
        }

        @Override
        public Node[] children() {
            return null;
        }

        private int[] mergeSort(LiveHashTrie trie, int[] data, int[] log, int logCount) {
            int dataCount = data.length;

            var res = IntStream.builder();
            var dataIdx = 0;
            var logIdx = 0;

            while (true) {
                if (dataIdx == dataCount) {
                    IntStream.range(logIdx, logCount).forEach(idx -> {
                        if (idx == logCount - 1 || trie.compare(log[idx], log[idx + 1]) != 0) {
                            res.add(log[idx]);
                        }
                    });
                    break;
                }

                if (logIdx == logCount) {
                    IntStream.range(dataIdx, dataCount).forEach(idx -> res.add(data[idx]));
                    break;
                }

                var dataKey = data[dataIdx];
                var logKey = log[logIdx];

                // this collapses down multiple duplicate values within the log
                if (logIdx + 1 < logCount && trie.compare(logKey, log[logIdx + 1]) == 0) {
                    logIdx++;
                    continue;
                }

                switch (Integer.signum(trie.compare(dataKey, logKey))) {
                    case -1 -> {
                        res.add(dataKey);
                        dataIdx++;
                    }
                    case 0 -> {
                        res.add(logKey);
                        dataIdx++;
                        logIdx++;
                    }
                    case 1 -> {
                        res.add(logKey);
                        logIdx++;
                    }
                }
            }

            return res.build().toArray();
        }

        private int[] sortLog(LiveHashTrie trie, int[] log, int logCount) {
            // this is a little convoluted, but AFAICT this is the only way to guarantee a 'stable' sort,
            // (`Stream.sorted()` doesn't guarantee it), which is required for the log (to preserve insertion order)
            var boxedArray = Arrays.stream(log).limit(logCount).boxed().toArray(Integer[]::new);
            Arrays.sort(boxedArray, trie::compare);
            return Arrays.stream(boxedArray).mapToInt(i -> i).toArray();
        }

        private int[][] idxBuckets(LiveHashTrie trie, int[] idxs, byte[] path) {
            var entryGroups = new IntStream.Builder[LEVEL_WIDTH];
            for (int i : idxs) {
                int groupIdx = trie.bucketFor(i, path.length);
                var group = entryGroups[groupIdx];
                if (group == null) {
                    entryGroups[groupIdx] = group = IntStream.builder();
                }

                group.add(i);
            }

            return Arrays.stream(entryGroups).map(b -> b == null ? null : b.build().toArray()).toArray(int[][]::new);
        }

        @Override
        public Node compactLogs(LiveHashTrie trie) {
            if (logCount == 0) return this;

            var data = mergeSort(trie, this.data, sortLog(trie, log, logCount), logCount);
            var log = new int[this.logLimit];
            var logCount = 0;

            if (data.length > this.pageLimit) {
                var childBuckets = idxBuckets(trie, data, path);

                var childNodes = IntStream.range(0, childBuckets.length)
                        .mapToObj(childIdx -> {
                            var childBucket = childBuckets[childIdx];
                            return childBucket == null ? null : new Leaf(logLimit, pageLimit, conjPath(path, (byte) childIdx), childBucket);
                        }).toArray(Node[]::new);

                return new Branch(logLimit, pageLimit, path, childNodes);
            } else {
                return new Leaf(logLimit, pageLimit, path, data, log, logCount);
            }
        }

        @Override
        public Node add(LiveHashTrie trie, int newIdx) {
            var data = this.data;
            var log = this.log;
            var logCount = this.logCount;
            log[logCount++] = newIdx;
            var newLeaf = new Leaf(logLimit, pageLimit, path, data, log, logCount);

            return logCount == this.logLimit ? newLeaf.compactLogs(trie) : newLeaf;
        }
    }
}
