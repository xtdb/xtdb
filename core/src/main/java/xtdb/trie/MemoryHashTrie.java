package xtdb.trie;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface MemoryHashTrie extends HashTrie {

    MemoryHashTrie add(int idx);

    record Config(TrieKeys keys, int logLimit, int pageLimit) {
        public static class Builder {
            private final TrieKeys keys;

            private int logLimit = 64;
            private int pageLimit = 1024;

            public Builder(TrieKeys keys) {
                this.keys = keys;
            }

            @SuppressWarnings("unused")
            public void setLogLimit(int logLimit) {
                this.logLimit = logLimit;
            }

            @SuppressWarnings("unused")
            public void setPageLimit(int pageLimit) {
                this.pageLimit = pageLimit;
            }

            public MemoryHashTrie build() {
                var trieConfig = new Config(keys, logLimit, pageLimit);
                return new Leaf(trieConfig, 0, new int[0], new int[logLimit], 0);
            }
        }
    }

    static Config.Builder builder(TrieKeys keys) {
        return new Config.Builder(keys);
    }

    @SuppressWarnings("unused")
    static MemoryHashTrie emptyTrie(TrieKeys keys) {
        return builder(keys).build();
    }

    record Branch(Config config, int level, MemoryHashTrie[] children) implements MemoryHashTrie {

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visitBranch(children);
        }

        @Override
        public MemoryHashTrie add(int idx) {
            var bucket = config.keys().groupFor(idx, level);

            var newChildren = IntStream.range(0, children.length)
                    .mapToObj(childIdx -> {
                        var child = children[childIdx];
                        if (bucket == childIdx) {
                            if (child == null) {
                                child = new MemoryHashTrie.Leaf(config, level + 1, new int[0], new int[config.logLimit()], 0);
                            }
                            child = child.add(idx);
                        }
                        return child;
                    }).toArray(MemoryHashTrie[]::new);

            return new Branch(config, level, newChildren);
        }
    }

    record Leaf(Config config, int level, int[] data, int[] log, int logCount) implements MemoryHashTrie {

        private int[] mergeSort(int[] data, int[] log, int logCount) {
            TrieKeys keys = config.keys();
            int dataCount = data.length;

            var res = IntStream.builder();
            var dataIdx = 0;
            var logIdx = 0;

            while (true) {
                if (dataIdx == dataCount) {
                    IntStream.range(logIdx, logCount).forEach(idx -> {
                        if (idx == logCount - 1 || keys.compare(log[idx], log[idx + 1]) != 0) {
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
                if (logIdx + 1 < logCount && keys.compare(logKey, log[logIdx + 1]) == 0) {
                    logIdx++;
                    continue;
                }

                switch (Integer.signum(keys.compare(dataKey, logKey))) {
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

        private int[] sortLog(int[] log, int logCount) {
            // this is a little convoluted, but AFAICT this is the only way to guarantee a 'stable' sort,
            // (`Stream.sorted()` doesn't guarantee it), which is required for the log (to preserve insertion order)
            var boxedArray = Arrays.stream(log).limit(logCount).boxed().toArray(Integer[]::new);
            Arrays.sort(boxedArray, config.keys::compare);
            return Arrays.stream(boxedArray).mapToInt(i -> i).toArray();
        }

        private Stream<int[]> idxBuckets(int[] idxs, int level) {
            var entryGroups = new IntStream.Builder[LEVEL_WIDTH];
            for (int i : idxs) {
                int groupIdx = config.keys().groupFor(i, level);
                var group = entryGroups[groupIdx];
                if (group == null) {
                    entryGroups[groupIdx] = group = IntStream.builder();
                }

                group.add(i);
            }

            return Arrays.stream(entryGroups).map(b -> b == null ? null : b.build().toArray());
        }

        @Override
        public MemoryHashTrie add(int newIdx) {
            var data = this.data;
            var log = this.log;
            var logCount = this.logCount;
            var logLimit = config.logLimit();
            log[logCount++] = newIdx;

            if (logCount == logLimit) {
                data = mergeSort(data, sortLog(log, logCount), logCount);
                log = new int[logLimit];
                logCount = 0;

                if (data.length > config.pageLimit()) {
                    var childNodes = idxBuckets(data, level)
                            .map(group -> group == null ? null : new MemoryHashTrie.Leaf(config, level + 1, group, new int[logLimit], 0))
                            .toArray(MemoryHashTrie[]::new);

                    return new Branch(config, level, childNodes);
                }
            }

            return new MemoryHashTrie.Leaf(config, level, data, log, logCount);
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            var data = mergeSort(this.data, sortLog(log, logCount), logCount);

            return visitor.visitLeaf(-1, data);
        }
    }
}
