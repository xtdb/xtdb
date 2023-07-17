package xtdb.trie;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static xtdb.trie.TrieKeys.LEVEL_WIDTH;

public record LiveTrie(Node rootNode, TrieKeys trieKeys) {

    private static final int LOG_LIMIT = 64;
    private static final int PAGE_LIMIT = 1024;

    public interface NodeVisitor<R> {
        R visitBranch(Branch branch);

        R visitLeaf(Leaf leaf);
    }

    public sealed interface Node {
        byte[] path();

        Node add(LiveTrie trie, int idx);

        Node compactLogs(LiveTrie trie);

        <R> R accept(NodeVisitor<R> visitor);
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

        public LiveTrie build() {
            return new LiveTrie(new Leaf(logLimit, pageLimit), trieKeys);
        }
    }

    public static Builder builder(TrieKeys trieKeys) {
        return new Builder(trieKeys);
    }

    @SuppressWarnings("unused")
    public static LiveTrie emptyTrie(TrieKeys trieKeys) {
        return new LiveTrie(new Leaf(LOG_LIMIT, PAGE_LIMIT), trieKeys);
    }

    public LiveTrie add(int idx) {
        return new LiveTrie(rootNode.add(this, idx), trieKeys);
    }

    public LiveTrie compactLogs() {
        return new LiveTrie(rootNode.compactLogs(this), trieKeys);
    }

    private int bucketFor(int idx, int level) {
        return trieKeys.bucketFor(idx, level);
    }

    private int compare(int leftIdx, int rightIdx) {
        return trieKeys.compare(leftIdx, rightIdx);
    }

    public Leaf[] getLeaves() {
        return accept(new NodeVisitor<Stream<Leaf>>() {
            @Override
            public Stream<Leaf> visitBranch(Branch branch) {
                return Arrays.stream(branch.children).flatMap(n -> n == null ? Stream.empty() : n.accept(this));
            }

            @Override
            public Stream<Leaf> visitLeaf(Leaf leaf) {
                return Stream.of(leaf);
            }
        }).toArray(Leaf[]::new);
    }

    public <R> R accept(NodeVisitor<R> visitor) {
        return rootNode.accept(visitor);
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
        public <R> R accept(NodeVisitor<R> visitor) {
            return visitor.visitBranch(this);
        }

        @Override
        public Node add(LiveTrie trie, int idx) {
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
        public Node compactLogs(LiveTrie trie) {
            Node[] children =
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

        private int[] mergeSort(LiveTrie trie, int[] data, int[] log, int logCount) {
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

        private int[] sortLog(LiveTrie trie, int[] log, int logCount) {
            // this is a little convoluted, but AFAICT this is the only way to guarantee a 'stable' sort,
            // (`Stream.sorted()` doesn't guarantee it), which is required for the log (to preserve insertion order)
            var boxedArray = Arrays.stream(log).limit(logCount).boxed().toArray(Integer[]::new);
            Arrays.sort(boxedArray, trie::compare);
            return Arrays.stream(boxedArray).mapToInt(i -> i).toArray();
        }

        private int[][] idxBuckets(LiveTrie trie, int[] idxs, byte[] path) {
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
        public Node compactLogs(LiveTrie trie) {
            if (logCount == 0) return this;

            var data = mergeSort(trie, this.data, sortLog(trie, log, logCount), logCount);
            var log = new int[this.logLimit];
            var logCount = 0;

            if (data.length > this.pageLimit) {
                var childBuckets = idxBuckets(trie, data, path);

                var childNodes = new Node[childBuckets.length];
                for (int childIdx = 0; childIdx < childBuckets.length; childIdx++) {
                    var childBucket = childBuckets[childIdx];
                    childNodes[childIdx] = childBucket == null ? null : new Leaf(logLimit, pageLimit, conjPath(path, (byte) childIdx), childBucket);
                }

                return new Branch(logLimit, pageLimit, path, childNodes);
            } else {
                return new Leaf(logLimit, pageLimit, path, data, log, logCount);
            }
        }

        @Override
        public Node add(LiveTrie trie, int newIdx) {
            var data = this.data;
            var log = this.log;
            var logCount = this.logCount;
            log[logCount++] = newIdx;
            var newLeaf = new Leaf(logLimit, pageLimit, path, data, log, logCount);

            return logCount == this.logLimit ? newLeaf.compactLogs(trie) : newLeaf;
        }

        @Override
        public <R> R accept(NodeVisitor<R> visitor) {
            return visitor.visitLeaf(this);
        }
    }
}
