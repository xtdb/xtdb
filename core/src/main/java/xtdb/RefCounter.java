package xtdb;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

public class RefCounter {
    private final StampedLock lock = new StampedLock();
    private AtomicInteger refs = new AtomicInteger(0);
    private volatile Semaphore closingSemaphore;

    public void acquire() {
        var stamp = lock.readLock();
        try {
            if (closingSemaphore == null) {
                refs.incrementAndGet();
            } else {
                throw new IllegalStateException("node closing");
            }
        } finally {
            lock.unlock(stamp);
        }
    }

    public void release() {
        var stamp = lock.readLock();
        try {
            refs.decrementAndGet();
            if (closingSemaphore != null) {
                closingSemaphore.release();
            }
        } finally {
            lock.unlock(stamp);
        }
    }

    public boolean isClosing() {
        return closingSemaphore != null;
    }

    public boolean tryClose(Duration timeout) throws Exception {
        int openQueries;

        var stamp = lock.writeLock();
        try {
            if (closingSemaphore != null) {
                throw new IllegalStateException("node closing");
            } else {
                openQueries = refs.get();
                closingSemaphore = new Semaphore(0);
            }
        } finally {
            lock.unlock(stamp);
        }

        if (timeout != null) {
            return closingSemaphore.tryAcquire(openQueries, timeout.toSeconds(), TimeUnit.SECONDS);
        } else {
            return closingSemaphore.tryAcquire(openQueries);
        }
    }
}
