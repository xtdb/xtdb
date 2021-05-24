package core2.temporal;

import java.io.Closeable;
import java.io.IOException;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import clojure.lang.Murmur3;

public interface IBlockCache extends Closeable {
    FixedSizeListVector getBlockVector(int blockIdx);

    public static final class LatestBlockCache implements IBlockCache {
        private final IBlockCache blockCache;
        private int latestBlockIdx = -1;
        private FixedSizeListVector latestBlock = null;

        public LatestBlockCache(IBlockCache blockCache) {
            this.blockCache = blockCache;
        }

        public FixedSizeListVector getBlockVector(final int blockIdx) {
            if (blockIdx == latestBlockIdx) {
                return latestBlock;
            } else {
                latestBlockIdx = blockIdx;
                latestBlock = blockCache.getBlockVector(blockIdx);
                return latestBlock;
            }
        }

        public void close() throws IOException {
            if (latestBlock != null) {
                latestBlock.close();
            }
            blockCache.close();
        }
    }

    public static final class ClockBlockCache implements IBlockCache {
        private final int[] keys;
        private final byte[] tags;
        private final FixedSizeListVector[] values;
        private final boolean[] referenced;
        private final IBlockCache blockCache;
        private final int size;
        private final int mask;
        private int clock;
        private int used;

        public ClockBlockCache(int size, IBlockCache blockCache) {
            this.blockCache = blockCache;
            this.keys = new int[size];
            this.tags = new byte[size];
            this.values = new FixedSizeListVector[size];
            this.referenced = new boolean[size];
            this.size = size;
            this.mask = size - 1;
            this.used = 0;
        }

        public FixedSizeListVector getBlockVector(final int blockIdx) {
            final byte tag = (byte) blockIdx;
            for (int i = 0; i < used; i++) {
                if (tags[i] == tag && keys[i] == blockIdx) {
                    final FixedSizeListVector v = values[i];
                    if (v != null) {
                        referenced[i] = true;
                        return v;
                    }
                }
            }
            while (true) {
                if (!referenced[clock]) {
                    FixedSizeListVector v = values[clock];
                    if (v != null) {
                        v.close();
                    }
                    tags[clock] = tag;
                    keys[clock] = blockIdx;
                    v = blockCache.getBlockVector(blockIdx);
                    values[clock] = v;
                    clock++;
                    used = Math.max(clock, used);
                    if (clock == size) {
                        clock = 0;
                    }
                    return v;
                } else {
                    referenced[clock] = false;
                    clock++;
                    if (clock == size) {
                        clock = 0;
                    }
                }
            }
        }

        public void close() throws IOException {
            for (FixedSizeListVector v : values) {
                if (v != null) {
                    v.close();
                }
            }
            blockCache.close();
        }
    }
}
