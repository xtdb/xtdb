package xtdb;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * Provides synchronization for LMDB map resizes, see lmdb.clj (new-mapsize-sync).
 * This class is an implementation detail of the lmdb module and may change without notice.
 *
 *
 * Magic numbers:
 *
 * Lock state (atomic int):
 * -1 write lock held for resize
 * 0 no open transactions, no resize lock (default)
 * 1+ number of open transactions
 *
 * Signal arg to acquire/release methods is 1 for 'open transaction' or -1 for 'map resize lock'.
 */
public class MapResizeSync extends AbstractQueuedSynchronizer {

    private static final long serialVersionUID = -1672790154983593262L;

    // Notes:
    // I wanted to avoid the .java, but couldn't easily use (proxy) in clj because protected methods cannot be called.

    // Reason for queued synchronizer to get fairness policy and platform blocking for LMDB locks without implementing our own spin.
    // originally the stamped lock that was in use did not enforce fairness / write priority which could cause ingestion to wait too long
    // to get lock.
    public MapResizeSync() {
        super();
    }

    @Override
    protected boolean tryAcquire(int signal) {
        // note: not re-entrant, not yet necessary
        assert signal == -1;
        if (compareAndSetState(0, -1)) {
            setExclusiveOwnerThread(Thread.currentThread());
            return true;
        }
        return false;
    }

    @Override
    protected boolean tryRelease(int signal) {
        assert signal == -1;
        if (!isHeldExclusively()) {
            throw new IllegalMonitorStateException("Map lock released on-non owner thread");
        }
        setState(0);
        setExclusiveOwnerThread(null);
        return true;
    }

    @Override
    protected boolean isHeldExclusively() {
        return getExclusiveOwnerThread() == Thread.currentThread();
    }
    @Override
    protected int tryAcquireShared(int signal) {
        assert signal == 1;
        if (hasQueuedPredecessors()) {
            return -1;
        }
        int st = getState();
        if (st >= 0 && compareAndSetState(st, st+1)) {
            return st+1;
        }
        return -1;
    }

    @Override
    protected boolean tryReleaseShared(int signal) {
        assert signal == 1;
        for(;;) {
            int st = getState();
            if (st < 1) {
                throw new IllegalMonitorStateException("Release of map resize read lock did not correspond to an acquire.");
            }
            if (compareAndSetState(st, st-1)) {
                return true;
            }
        }
    }
}
