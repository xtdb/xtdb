package xtdb.trie;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static xtdb.trie.TrieKeys.LEVEL_WIDTH;

public interface MemoryHashTrie {

    int LOG_LIMIT = 64;
    int PAGE_LIMIT = 1024;

    interface Visitor<R> {
        R visitBranch(Branch branch);
        R visitLeaf(Leaf leaf);
    }

    MemoryHashTrie add(TrieKeys trieKeys, int idx);

    MemoryHashTrie compactLogs(TrieKeys trieKeys);

    <R> R accept(Visitor<R> visitor);

    @SuppressWarnings("unused")
    static MemoryHashTrie emptyTrie() {
        return new Leaf();
    }

    record Branch(int level, MemoryHashTrie[] children) implements MemoryHashTrie {

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visitBranch(this);
        }

        @Override
        public MemoryHashTrie add(TrieKeys trieKeys, int idx) {
            var bucket = trieKeys.bucketFor(idx, level);

            var newChildren = IntStream.range(0, children.length)
                    .mapToObj(childIdx -> {
                        var child = children[childIdx];
                        if (bucket == childIdx) {
                            if (child == null) {
                                child = new Leaf(level + 1);
                            }
                            child = child.add(trieKeys, idx);
                        }
                        return child;
                    }).toArray(MemoryHashTrie[]::new);

            return new Branch(level, newChildren);
        }

        @Override
        public MemoryHashTrie compactLogs(TrieKeys trieKeys) {
            MemoryHashTrie[] children =
                    Arrays.stream(this.children)
                            .map(child -> child == null ? null : child.compactLogs(trieKeys))
                            .toArray(MemoryHashTrie[]::new);

            return new Branch(level, children);
        }
    }

    record Leaf(int level, int[] data, int[] log, int logCount) implements MemoryHashTrie {

        private Leaf() {
            this(0);
        }

        private Leaf(int level) {
            this(level, new int[0]);
        }

        private Leaf(int level, int[] data) {
            this(level, data, new int[LOG_LIMIT], 0);
        }

        private int[] mergeSort(TrieKeys trieKeys, int[] data, int[] log, int logCount) {
            int dataCount = data.length;

            var res = IntStream.builder();
            var dataIdx = 0;
            var logIdx = 0;

            while (true) {
                if (dataIdx == dataCount) {
                    IntStream.range(logIdx, logCount).forEach(idx -> {
                        if (idx == logCount - 1 || trieKeys.compare(log[idx], log[idx + 1]) != 0) {
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
                if (logIdx + 1 < logCount && trieKeys.compare(logKey, log[logIdx + 1]) == 0) {
                    logIdx++;
                    continue;
                }

                switch (Integer.signum(trieKeys.compare(dataKey, logKey))) {
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

        private int[] sortLog(TrieKeys trieKeys, int[] log, int logCount) {
            // this is a little convoluted, but AFAICT this is the only way to guarantee a 'stable' sort,
            // (`Stream.sorted()` doesn't guarantee it), which is required for the log (to preserve insertion order)
            var boxedArray = Arrays.stream(log).limit(logCount).boxed().toArray(Integer[]::new);
            Arrays.sort(boxedArray, trieKeys::compare);
            return Arrays.stream(boxedArray).mapToInt(i -> i).toArray();
        }

        private Stream<int[]> idxBuckets(TrieKeys trieKeys, int[] idxs, int level) {
            var entryGroups = new IntStream.Builder[LEVEL_WIDTH];
            for (int i : idxs) {
                int groupIdx = trieKeys.bucketFor(i, level);
                var group = entryGroups[groupIdx];
                if (group == null) {
                    entryGroups[groupIdx] = group = IntStream.builder();
                }

                group.add(i);
            }

            return Arrays.stream(entryGroups).map(b -> b == null ? null : b.build().toArray());
        }

        @Override
        public MemoryHashTrie compactLogs(TrieKeys trieKeys) {
            if (logCount == 0) return this;

            var data = mergeSort(trieKeys, this.data, sortLog(trieKeys, log, logCount), logCount);
            var log = new int[LOG_LIMIT];
            var logCount = 0;

            if (data.length > PAGE_LIMIT) {
                var childNodes = idxBuckets(trieKeys, data, level)
                        .map(group -> group == null ? null : new MemoryHashTrie.Leaf(level + 1, group))
                        .toArray(MemoryHashTrie[]::new);

                return new Branch(level, childNodes);
            } else {
                return new Leaf(level, data, log, logCount);
            }
        }

        @Override
        public MemoryHashTrie add(TrieKeys trieKeys, int newIdx) {
            var data = this.data;
            var log = this.log;
            var logCount = this.logCount;
            log[logCount++] = newIdx;
            var newLeaf = new Leaf(level, data, log, logCount);

            return logCount == LOG_LIMIT ? newLeaf.compactLogs(trieKeys) : newLeaf;
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visitLeaf(this);
        }
    }
}
