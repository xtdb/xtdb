package core2.temporal;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.Spliterator;

public final class SubtreeSpliterator implements Spliterator.OfLong, LongSupplier {
    private final long n;
    private long currentInLevel;
    private long maxInLevel;
    private long current;

    public SubtreeSpliterator(final long currentInLevel, final long maxInLevel, final long current, final long n) {
        this.currentInLevel = currentInLevel;
        this.maxInLevel = maxInLevel;
        this.current = current;
        this.n = n;
    }

    public Object clone() {
        return new SubtreeSpliterator(currentInLevel, maxInLevel, current, n);
    }

    public boolean tryAdvance(final LongConsumer consumer) {
        if (current < n) {
            consumer.accept(current);
            current++;
            currentInLevel++;
            if (currentInLevel == maxInLevel) {
                current = (((current - maxInLevel) << 1) + 1);
                maxInLevel += maxInLevel;
                currentInLevel = 0;
            }
            return current < n;
        }
        return false;
    }

    public Spliterator.OfLong trySplit() {
        return null;
    }

    public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL| Spliterator.ORDERED;
    }

    public long estimateSize() {
        return -1;
    }

    public long getAsLong() {
        return current;
    }
}
